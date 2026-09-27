// Spring Kafka against noida-db: KafkaTemplate (send with a callback, and
// sendDefault), a ConcurrentMessageListenerContainer (the @KafkaListener
// machinery without needing a full Spring Boot app context), manual ack
// mode, and a transactional KafkaTemplate. This is the API surface most
// Spring applications actually use, distinct from raw kafka-clients.
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.kafka.core.*;
import org.springframework.kafka.listener.*;
import org.springframework.kafka.support.SendResult;
import org.springframework.kafka.transaction.KafkaTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import java.util.*;
import java.util.concurrent.*;

public class SpringKafkaTest {
    public static void main(String[] args) throws Exception {
        String port = System.getenv("NOIDA_KAFKA_PORT");
        String bootstrap = "127.0.0.1:" + port;

        // --- KafkaTemplate: produce, with the future/callback API Spring apps use ---
        Map<String, Object> producerProps = new HashMap<>();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        DefaultKafkaProducerFactory<String, String> pf = new DefaultKafkaProducerFactory<>(producerProps);
        KafkaTemplate<String, String> template = new KafkaTemplate<>(pf);
        template.setDefaultTopic("spring-topic");

        SendResult<String, String> result = template.send("spring-topic", "k1", "v1").get(10, TimeUnit.SECONDS);
        Check.check("send result has an offset", result.getRecordMetadata().offset() >= 0, true);
        template.sendDefault("k2", "v2").get(10, TimeUnit.SECONDS);

        // --- ConcurrentMessageListenerContainer: what @KafkaListener wires up ---
        Map<String, Object> consumerProps = new HashMap<>();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "spring-group");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        DefaultKafkaConsumerFactory<String, String> cf = new DefaultKafkaConsumerFactory<>(consumerProps);

        ContainerProperties containerProps = new ContainerProperties("spring-topic");
        containerProps.setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
        List<String> received = Collections.synchronizedList(new ArrayList<>());
        CountDownLatch latch = new CountDownLatch(2);
        containerProps.setMessageListener((AcknowledgingMessageListener<String, String>) (record, ack) -> {
            received.add(record.value());
            ack.acknowledge();
            latch.countDown();
        });

        ConcurrentMessageListenerContainer<String, String> container =
                new ConcurrentMessageListenerContainer<>(cf, containerProps);
        container.start();
        boolean gotAll = latch.await(15, TimeUnit.SECONDS);
        CountDownLatch stopped = new CountDownLatch(1);
        container.stop(stopped::countDown); // stop() is async; wait for the real shutdown
        stopped.await(10, TimeUnit.SECONDS);
        Check.check("listener container received both messages", gotAll, true);
        List<String> firstTwo = new ArrayList<>(received.subList(0, Math.min(2, received.size())));
        Collections.sort(firstTwo);
        Check.check("listener container values", firstTwo, Arrays.asList("v1", "v2"));

        // --- Transactional KafkaTemplate: produce inside a Spring-managed transaction ---
        Map<String, Object> txProducerProps = new HashMap<>(producerProps);
        txProducerProps.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "spring-tx-1");
        DefaultKafkaProducerFactory<String, String> txPf = new DefaultKafkaProducerFactory<>(txProducerProps);
        txPf.setTransactionIdPrefix("spring-tx-");
        KafkaTemplate<String, String> txTemplate = new KafkaTemplate<>(txPf);
        KafkaTransactionManager<String, String> txManager = new KafkaTransactionManager<>(txPf);
        TransactionTemplate tx = new TransactionTemplate(txManager);

        tx.execute(status -> {
            txTemplate.send("spring-topic", "k3", "committed");
            return null;
        });

        boolean[] caught = {false};
        try {
            tx.execute(status -> {
                try {
                    // Force the record onto the wire (and the broker's ack)
                    // before aborting, so this actually tests server-side
                    // abort visibility rather than a client-side buffer that
                    // never left the producer.
                    txTemplate.send("spring-topic", "k4", "aborted").get(10, TimeUnit.SECONDS);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
                throw new RuntimeException("force rollback");
            });
        } catch (RuntimeException e) {
            caught[0] = true;
        }
        Check.check("the aborting transaction's exception propagated", caught[0], true);

        // A read_committed consumer must see the committed message but not
        // the aborted one.
        Map<String, Object> rcProps = new HashMap<>(consumerProps);
        rcProps.put(ConsumerConfig.GROUP_ID_CONFIG, "spring-rc-group");
        rcProps.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        DefaultKafkaConsumerFactory<String, String> rcf = new DefaultKafkaConsumerFactory<>(rcProps);
        ContainerProperties rcContainerProps = new ContainerProperties("spring-topic");
        List<String> rcReceived = Collections.synchronizedList(new ArrayList<>());
        CountDownLatch rcLatch = new CountDownLatch(1);
        rcContainerProps.setMessageListener((MessageListener<String, String>) record -> {
            rcReceived.add(record.value());
            if ("k3".equals(record.key())) rcLatch.countDown();
        });
        ConcurrentMessageListenerContainer<String, String> rcContainer =
                new ConcurrentMessageListenerContainer<>(rcf, rcContainerProps);
        rcContainer.start();
        boolean gotCommitted = rcLatch.await(15, TimeUnit.SECONDS);
        Thread.sleep(1000); // give the aborted record a chance to (wrongly) show up too
        CountDownLatch rcStopped = new CountDownLatch(1);
        rcContainer.stop(rcStopped::countDown);
        rcStopped.await(10, TimeUnit.SECONDS);
        Check.check("read_committed consumer sees the committed record", gotCommitted, true);
        Check.check("read_committed consumer never sees the aborted record", rcReceived.contains("aborted"), false);

        pf.destroy();
        txPf.destroy();
        Check.report("spring-kafka");
    }
}
