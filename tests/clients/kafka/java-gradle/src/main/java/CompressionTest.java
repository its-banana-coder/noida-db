// Every compression codec kafka-clients supports, round-tripped through
// noida-db: the spec requires each to be decoded and kept as sent, since
// noida-db doesn't re-encode (single-node, no need to normalize). A
// mismatch here means the record batch parser or the codec itself is wrong.
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.util.*;

public class CompressionTest {
    public static void main(String[] args) throws Exception {
        String port = System.getenv("NOIDA_KAFKA_PORT");
        String bootstrap = "127.0.0.1:" + port;

        for (String codec : new String[]{"none", "gzip", "snappy", "lz4", "zstd"}) {
            String topic = "compression-" + codec;
            Properties producerProps = new Properties();
            producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
            producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            producerProps.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, codec);
            // A big-ish, repetitive payload so a real codec actually does something,
            // rather than falling back to storing it uncompressed.
            String payload = ("noida-db ".repeat(200)) + codec;

            List<String> sent = Arrays.asList(payload + "-1", payload + "-2", payload + "-3");
            try (Producer<String, String> producer = new KafkaProducer<>(producerProps)) {
                for (String v : sent) {
                    producer.send(new ProducerRecord<>(topic, "k", v)).get();
                }
            }

            Properties consumerProps = new Properties();
            consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
            consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "compression-group-" + codec);
            consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

            List<String> received = new ArrayList<>();
            try (Consumer<String, String> consumer = new KafkaConsumer<>(consumerProps)) {
                consumer.subscribe(Collections.singletonList(topic));
                long start = System.currentTimeMillis();
                while (System.currentTimeMillis() - start < 10000 && received.size() < 3) {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
                    for (ConsumerRecord<String, String> record : records) {
                        received.add(record.value());
                    }
                }
            }
            Check.check(codec + ": round trip", received, sent);
        }

        Check.report("compression-codecs");
    }
}
