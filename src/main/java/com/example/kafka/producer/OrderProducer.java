package com.example.kafka.producer;

import com.example.kafka.model.Order;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;

@Slf4j
@Service
@RequiredArgsConstructor
public class OrderProducer {

    private final KafkaTemplate<String, Order> orderKafkaTemplate;

    @Value("${kafka.topics.orders}")
    private String ordersTopic;

    public void sendOrder(Order order) {
        CompletableFuture<SendResult<String, Order>> future =
                orderKafkaTemplate.send(ordersTopic, order.getOrderId(), order);

        future.whenComplete((result, ex) -> {
            if (ex == null) {
                log.info("Order sent successfully: orderId={}, partition={}, offset={}",
                        order.getOrderId(),
                        result.getRecordMetadata().partition(),
                        result.getRecordMetadata().offset());
            } else {
                log.error("Failed to send order: orderId={}", order.getOrderId(), ex);
            }
        });
    }

    public void sendOrderWithHeaders(Order order, String source) {
        ProducerRecord<String, Order> record = new ProducerRecord<>(
                ordersTopic,
                null,   // partition (let Kafka decide)
                order.getOrderId(),
                order
        );
        record.headers().add(new RecordHeader("source", source.getBytes(StandardCharsets.UTF_8)));
        record.headers().add(new RecordHeader("version", "1.0".getBytes(StandardCharsets.UTF_8)));

        CompletableFuture<SendResult<String, Order>> future = orderKafkaTemplate.send(record);
        future.whenComplete((result, ex) -> {
            if (ex == null) {
                log.info("Order sent with headers: orderId={}, source={}", order.getOrderId(), source);
            } else {
                log.error("Failed to send order with headers: orderId={}", order.getOrderId(), ex);
            }
        });
    }

    public CompletableFuture<SendResult<String, Order>> sendOrderAsync(Order order) {
        return orderKafkaTemplate.send(ordersTopic, order.getOrderId(), order);
    }

    public void sendOrderToPartition(Order order, int partition) {
        ProducerRecord<String, Order> record = new ProducerRecord<>(
                ordersTopic, partition, order.getOrderId(), order
        );
        orderKafkaTemplate.send(record).whenComplete((result, ex) -> {
            if (ex == null) {
                log.info("Order sent to partition {}: orderId={}", partition, order.getOrderId());
            } else {
                log.error("Failed to send order to partition {}: orderId={}", partition, order.getOrderId(), ex);
            }
        });
    }
}
