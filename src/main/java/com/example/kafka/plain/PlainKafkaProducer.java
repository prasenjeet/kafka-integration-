package com.example.kafka.plain;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * Plain Kafka producer using the raw kafka-clients API (no Spring).
 * Demonstrates synchronous, asynchronous, and fire-and-forget patterns.
 */
@Slf4j
public class PlainKafkaProducer implements AutoCloseable {

    private final KafkaProducer<String, String> producer;
    private final String topic;

    public PlainKafkaProducer(String bootstrapServers, String topic) {
        this.topic = topic;
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 3);
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        props.put(ProducerConfig.LINGER_MS_CONFIG, 5);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
        this.producer = new KafkaProducer<>(props);
    }

    /** Fire-and-forget: send without waiting for acknowledgment. */
    public void sendFireAndForget(String key, String value) {
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);
        producer.send(record);
        log.debug("Fire-and-forget sent: key={}", key);
    }

    /** Synchronous send: blocks until the broker acknowledges. */
    public RecordMetadata sendSync(String key, String value) throws InterruptedException, ExecutionException {
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);
        RecordMetadata metadata = producer.send(record).get();
        log.info("Sync send complete: key={}, partition={}, offset={}", key, metadata.partition(), metadata.offset());
        return metadata;
    }

    /** Asynchronous send with a callback. */
    public void sendAsync(String key, String value) {
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);
        producer.send(record, (metadata, exception) -> {
            if (exception == null) {
                log.info("Async send complete: key={}, partition={}, offset={}", key, metadata.partition(), metadata.offset());
            } else {
                log.error("Async send failed: key={}", key, exception);
            }
        });
    }

    /** Flush all buffered messages to the broker. */
    public void flush() {
        producer.flush();
    }

    @Override
    public void close() {
        producer.close();
        log.info("Producer closed");
    }

    public static void main(String[] args) throws Exception {
        String bootstrapServers = "localhost:9092";
        String topic = "test-topic";

        try (PlainKafkaProducer producer = new PlainKafkaProducer(bootstrapServers, topic)) {
            // Fire-and-forget
            producer.sendFireAndForget("key-1", "Hello, Kafka! (fire-and-forget)");

            // Async
            producer.sendAsync("key-2", "Hello, Kafka! (async)");

            // Sync
            producer.sendSync("key-3", "Hello, Kafka! (sync)");

            producer.flush();
            System.out.println("All messages sent.");
        }
    }
}
