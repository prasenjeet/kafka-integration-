package com.example.kafka.plain;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.*;

/**
 * Plain Kafka consumer using the raw kafka-clients API (no Spring).
 * Demonstrates manual offset commit and graceful shutdown.
 */
@Slf4j
public class PlainKafkaConsumer implements AutoCloseable {

    private final KafkaConsumer<String, String> consumer;
    private volatile boolean running = true;

    public PlainKafkaConsumer(String bootstrapServers, String groupId, List<String> topics) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 100);
        props.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, 1);
        props.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, 500);

        this.consumer = new KafkaConsumer<>(props);
        this.consumer.subscribe(topics, new ConsumerRebalanceListener() {
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                log.info("Partitions revoked: {}", partitions);
                consumer.commitSync();
            }

            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                log.info("Partitions assigned: {}", partitions);
            }
        });
    }

    /** Polls and processes messages until stopped. Commits offsets manually per batch. */
    public void startPolling() {
        try {
            while (running) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

                if (records.isEmpty()) {
                    continue;
                }

                log.debug("Polled {} records", records.count());

                for (ConsumerRecord<String, String> record : records) {
                    processRecord(record);
                }

                // Synchronous commit after processing the batch
                consumer.commitSync();
                log.debug("Offsets committed for batch of {}", records.count());
            }
        } catch (WakeupException e) {
            if (running) throw e; // unexpected
            log.info("Consumer woken up for shutdown");
        } finally {
            consumer.commitSync();
            consumer.close();
            log.info("Consumer closed");
        }
    }

    private void processRecord(ConsumerRecord<String, String> record) {
        log.info("Consumed: topic={}, partition={}, offset={}, key={}, value={}",
                record.topic(), record.partition(), record.offset(), record.key(), record.value());
    }

    /** Seek to the beginning of all assigned partitions. */
    public void seekToBeginning() {
        consumer.seekToBeginning(consumer.assignment());
    }

    /** Seek to a specific offset for a partition. */
    public void seekToOffset(String topic, int partition, long offset) {
        consumer.seek(new TopicPartition(topic, partition), offset);
    }

    /** Gracefully shuts down the polling loop. */
    public void stop() {
        running = false;
        consumer.wakeup();
    }

    @Override
    public void close() {
        stop();
    }

    public static void main(String[] args) {
        String bootstrapServers = "localhost:9092";
        String groupId = "plain-consumer-group";
        List<String> topics = List.of("test-topic");

        PlainKafkaConsumer consumer = new PlainKafkaConsumer(bootstrapServers, groupId, topics);

        // Graceful shutdown on JVM exit
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Shutdown hook triggered — stopping consumer...");
            consumer.stop();
        }));

        System.out.println("Starting consumer. Press Ctrl+C to stop.");
        consumer.startPolling();
    }
}
