package com.example.kafka.admin;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.TopicPartitionInfo;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.concurrent.ExecutionException;

@Slf4j
@Service
@RequiredArgsConstructor
public class KafkaAdminService {

    private final KafkaAdmin kafkaAdmin;

    public Map<String, TopicDescription> describeTopics(List<String> topicNames) {
        try (AdminClient adminClient = AdminClient.create(kafkaAdmin.getConfigurationProperties())) {
            DescribeTopicsResult result = adminClient.describeTopics(topicNames);
            return result.allTopicNames().get();
        } catch (InterruptedException | ExecutionException e) {
            log.error("Failed to describe topics: {}", topicNames, e);
            Thread.currentThread().interrupt();
            return Collections.emptyMap();
        }
    }

    public Set<String> listTopics() {
        try (AdminClient adminClient = AdminClient.create(kafkaAdmin.getConfigurationProperties())) {
            ListTopicsResult result = adminClient.listTopics();
            return result.names().get();
        } catch (InterruptedException | ExecutionException e) {
            log.error("Failed to list topics", e);
            Thread.currentThread().interrupt();
            return Collections.emptySet();
        }
    }

    public void createTopic(String topicName, int partitions, short replicationFactor) {
        try (AdminClient adminClient = AdminClient.create(kafkaAdmin.getConfigurationProperties())) {
            NewTopic newTopic = new NewTopic(topicName, partitions, replicationFactor);
            CreateTopicsResult result = adminClient.createTopics(List.of(newTopic));
            result.all().get();
            log.info("Topic created: name={}, partitions={}, replication={}", topicName, partitions, replicationFactor);
        } catch (InterruptedException | ExecutionException e) {
            log.error("Failed to create topic: {}", topicName, e);
            Thread.currentThread().interrupt();
        }
    }

    public void deleteTopic(String topicName) {
        try (AdminClient adminClient = AdminClient.create(kafkaAdmin.getConfigurationProperties())) {
            adminClient.deleteTopics(List.of(topicName)).all().get();
            log.info("Topic deleted: {}", topicName);
        } catch (InterruptedException | ExecutionException e) {
            log.error("Failed to delete topic: {}", topicName, e);
            Thread.currentThread().interrupt();
        }
    }

    public void printTopicInfo(String topicName) {
        Map<String, TopicDescription> descriptions = describeTopics(List.of(topicName));
        TopicDescription desc = descriptions.get(topicName);
        if (desc == null) {
            log.warn("Topic not found: {}", topicName);
            return;
        }
        log.info("Topic: {}", desc.name());
        for (TopicPartitionInfo partition : desc.partitions()) {
            log.info("  Partition {}: leader={}, replicas={}, isr={}",
                    partition.partition(),
                    partition.leader(),
                    partition.replicas(),
                    partition.isr());
        }
    }

    public Map<String, Map<String, Long>> getConsumerGroupOffsets(String groupId) {
        try (AdminClient adminClient = AdminClient.create(kafkaAdmin.getConfigurationProperties())) {
            ListConsumerGroupOffsetsResult result = adminClient.listConsumerGroupOffsets(groupId);
            Map<String, Map<String, Long>> offsets = new HashMap<>();
            result.partitionsToOffsetAndMetadata().get().forEach((tp, om) -> {
                offsets.computeIfAbsent(tp.topic(), k -> new HashMap<>())
                        .put("partition-" + tp.partition(), om.offset());
            });
            return offsets;
        } catch (InterruptedException | ExecutionException e) {
            log.error("Failed to get consumer group offsets for group: {}", groupId, e);
            Thread.currentThread().interrupt();
            return Collections.emptyMap();
        }
    }
}
