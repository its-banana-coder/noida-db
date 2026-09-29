// A transactional kafka-clients producer against noida-db: initTransactions,
// beginTransaction, sendOffsetsToTransaction, commit and abort, and a
// read_committed consumer that must see committed records and skip aborted
// ones. Exercises InitProducerId, AddPartitionsToTxn, AddOffsetsToTxn,
// TxnOffsetCommit and EndTxn end to end (the engine has unit tests for each
// handler; this is the only place they're driven together by a real client).
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.util.*;

public class TransactionalProducerTest {
    public static void main(String[] args) throws Exception {
        String port = System.getenv("NOIDA_KAFKA_PORT");
        String bootstrap = "127.0.0.1:" + port;
        String topic = "txn-topic";

        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "txn-producer-1");
        producerProps.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");

        try (Producer<String, String> producer = new KafkaProducer<>(producerProps)) {
            producer.initTransactions();

            // A committed transaction: two records, one topic-partition offset commit.
            producer.beginTransaction();
            producer.send(new ProducerRecord<>(topic, "k1", "committed-1")).get();
            producer.send(new ProducerRecord<>(topic, "k1", "committed-2")).get();
            producer.commitTransaction();

            // An aborted transaction: a read_committed consumer must never see this.
            producer.beginTransaction();
            producer.send(new ProducerRecord<>(topic, "k1", "aborted")).get();
            producer.abortTransaction();

            // A committed transaction that also commits consumer offsets
            // (the exactly-once "consume-transform-produce" pattern).
            producer.beginTransaction();
            producer.send(new ProducerRecord<>(topic, "k1", "committed-3")).get();
            Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
            offsets.put(new TopicPartition("txn-topic", 0), new OffsetAndMetadata(5));
            producer.sendOffsetsToTransaction(offsets, "txn-consumer-group");
            producer.commitTransaction();

            // A second producer with the same transactional.id fences the first
            // (this is what protects against zombie producers after a restart).
            Properties producer2Props = new Properties();
            producer2Props.putAll(producerProps);
            try (Producer<String, String> producer2 = new KafkaProducer<>(producer2Props)) {
                producer2.initTransactions();
                producer2.beginTransaction();
                producer2.send(new ProducerRecord<>(topic, "k1", "from-producer-2")).get();
                producer2.commitTransaction();
            }
            boolean fenced = false;
            try {
                producer.beginTransaction();
                producer.send(new ProducerRecord<>(topic, "k1", "should be fenced")).get();
                producer.commitTransaction();
            } catch (ProducerFencedException | java.util.concurrent.ExecutionException e) {
                fenced = e instanceof ProducerFencedException || e.getCause() instanceof ProducerFencedException;
            }
            Check.check("the older transactional producer is fenced", fenced, true);
        }

        // read_committed: only committed-1, committed-2, committed-3 and
        // from-producer-2 should ever be delivered; "aborted" must not.
        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "txn-read-group");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProps.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");

        List<String> received = new ArrayList<>();
        try (Consumer<String, String> consumer = new KafkaConsumer<>(consumerProps)) {
            consumer.subscribe(Collections.singletonList(topic));
            long start = System.currentTimeMillis();
            while (System.currentTimeMillis() - start < 15000 && received.size() < 4) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
                for (ConsumerRecord<String, String> record : records) {
                    received.add(record.value());
                }
            }
        }
        Collections.sort(received);
        Check.check("read_committed sees exactly the committed records, in order",
                received, Arrays.asList("committed-1", "committed-2", "committed-3", "from-producer-2"));
        Check.check("the aborted record is never delivered", received.contains("aborted"), false);

        Check.report("transactional-producer");
    }
}
