import java.time.Duration;
import java.util.*;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

public class KafkaClientsTest {
    private static int checks = 0;
    private static final List<String> failures = new ArrayList<>();

    private static void check(String name, Object got, Object want) {
        checks++;
        if (!Objects.equals(got, want)) {
            failures.add(name + ": got " + got + ", want " + want);
        }
    }

    public static void main(String[] args) throws Exception {
        String port = System.getenv("NOIDA_KAFKA_PORT");
        if (port == null || port.isEmpty()) {
            System.err.println("NOIDA_KAFKA_PORT not set");
            System.exit(1);
        }
        String bootstrap = "127.0.0.1:" + port;

        // 1. AdminClient topic creation and description
        Properties adminProps = new Properties();
        adminProps.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        try (AdminClient admin = AdminClient.create(adminProps)) {
            String topic = "java-topic";
            admin.createTopics(Collections.singletonList(new NewTopic(topic, 3, (short) 1))).all().get();

            DescribeTopicsResult descResult = admin.describeTopics(Collections.singletonList(topic));
            TopicDescription desc = descResult.allTopicNames().get().get(topic);
            check("topic has 3 partitions", desc.partitions().size(), 3);
        }

        // 2. Producer: produce 3 keyed messages
        Properties prodProps = new Properties();
        prodProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        prodProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        prodProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        prodProps.put(ProducerConfig.ACKS_CONFIG, "all");
        prodProps.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");

        String topic = "java-topic";
        try (Producer<String, String> producer = new KafkaProducer<>(prodProps)) {
            producer.send(new ProducerRecord<>(topic, "k1", "v1")).get();
            producer.send(new ProducerRecord<>(topic, "k2", "v2")).get();
            producer.send(new ProducerRecord<>(topic, "k3", "v3")).get();
        }

        // 3. Consumer: consumer group consume and commit
        Properties consProps = new Properties();
        consProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        consProps.put(ConsumerConfig.GROUP_ID_CONFIG, "java-consumer-group");
        consProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        List<String> received = new ArrayList<>();
        try (Consumer<String, String> consumer = new KafkaConsumer<>(consProps)) {
            consumer.subscribe(Collections.singletonList(topic));
            long start = System.currentTimeMillis();
            while (System.currentTimeMillis() - start < 10000 && received.size() < 3) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
                for (ConsumerRecord<String, String> record : records) {
                    received.add(record.value());
                }
            }
            consumer.commitSync();
        }

        Collections.sort(received);
        check("consumed 3 messages", received, Arrays.asList("v1", "v2", "v3"));

        // 4. Resume past committed offsets with consumer 2
        try (Producer<String, String> producer = new KafkaProducer<>(prodProps)) {
            producer.send(new ProducerRecord<>(topic, "k4", "v4")).get();
        }

        List<String> resumed = new ArrayList<>();
        try (Consumer<String, String> consumer2 = new KafkaConsumer<>(consProps)) {
            consumer2.subscribe(Collections.singletonList(topic));
            long start = System.currentTimeMillis();
            while (System.currentTimeMillis() - start < 10000 && resumed.size() < 1) {
                ConsumerRecords<String, String> records = consumer2.poll(Duration.ofMillis(500));
                for (ConsumerRecord<String, String> record : records) {
                    resumed.add(record.value());
                }
            }
            consumer2.commitSync();
        }
        check("consumer 2 resumes past committed offset", resumed, Collections.singletonList("v4"));

        System.out.println("kafka-clients: " + checks + " checks, " + failures.size() + " failed");
        for (String f : failures) {
            System.out.println("  FAIL: " + f);
        }
        if (!failures.isEmpty()) {
            System.exit(1);
        }
    }
}
