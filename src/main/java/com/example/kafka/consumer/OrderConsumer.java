package com.example.kafka.consumer;

import com.example.kafka.model.Order;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.retry.annotation.Backoff;
import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;
import java.util.List;

@Slf4j
@Service
public class OrderConsumer {

    // Single record listener with manual ack
    @KafkaListener(
            topics = "${kafka.topics.orders}",
            groupId = "${spring.kafka.consumer.group-id}",
            containerFactory = "orderKafkaListenerContainerFactory"
    )
    public void consumeOrder(
            @Payload Order order,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset,
            Acknowledgment acknowledgment) {

        log.info("Received order: orderId={}, status={}, partition={}, offset={}",
                order.getOrderId(), order.getStatus(), partition, offset);

        try {
            processOrder(order);
            acknowledgment.acknowledge();
            log.debug("Order processed and acknowledged: orderId={}", order.getOrderId());
        } catch (Exception e) {
            log.error("Error processing order: orderId={}", order.getOrderId(), e);
            // Not acknowledging — message will be redelivered
        }
    }

    // Batch listener
    @KafkaListener(
            topics = "${kafka.topics.orders}",
            groupId = "batch-consumer-group",
            containerFactory = "orderKafkaListenerContainerFactory"
    )
    public void consumeOrderBatch(
            List<ConsumerRecord<String, Order>> records,
            Acknowledgment acknowledgment) {

        log.info("Received batch of {} orders", records.size());

        for (ConsumerRecord<String, Order> record : records) {
            Order order = record.value();
            log.debug("Processing batch order: orderId={}, partition={}, offset={}",
                    order.getOrderId(), record.partition(), record.offset());

            // Read custom headers if present
            Header sourceHeader = record.headers().lastHeader("source");
            if (sourceHeader != null) {
                String source = new String(sourceHeader.value(), StandardCharsets.UTF_8);
                log.debug("Order source: {}", source);
            }

            processOrder(order);
        }

        acknowledgment.acknowledge();
        log.info("Batch of {} orders acknowledged", records.size());
    }

    private void processOrder(Order order) {
        switch (order.getStatus()) {
            case PENDING -> log.info("Processing pending order: {}", order.getOrderId());
            case CONFIRMED -> log.info("Order confirmed, initiating fulfillment: {}", order.getOrderId());
            case SHIPPED -> log.info("Order shipped, updating tracking: {}", order.getOrderId());
            case DELIVERED -> log.info("Order delivered, closing: {}", order.getOrderId());
            case CANCELLED -> log.info("Order cancelled, processing refund: {}", order.getOrderId());
        }
    }
}
