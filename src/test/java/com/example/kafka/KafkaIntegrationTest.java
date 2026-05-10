package com.example.kafka;

import com.example.kafka.model.Order;
import com.example.kafka.producer.OrderProducer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.TestPropertySource;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
@DirtiesContext
@EmbeddedKafka(
        partitions = 1,
        topics = {"orders-topic", "payments-topic", "notifications-topic"},
        brokerProperties = {"listeners=PLAINTEXT://localhost:9093", "port=9093"}
)
@TestPropertySource(properties = {
        "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}",
        "spring.kafka.consumer.auto-offset-reset=earliest"
})
class KafkaIntegrationTest {

    @Autowired
    private OrderProducer orderProducer;

    private final BlockingQueue<Order> receivedOrders = new LinkedBlockingQueue<>();

    @KafkaListener(topics = "orders-topic", groupId = "test-group")
    void listenForTestOrders(ConsumerRecord<String, Order> record) {
        receivedOrders.offer(record.value());
    }

    @Test
    void shouldSendAndReceiveOrder() throws InterruptedException {
        Order order = Order.builder()
                .orderId(UUID.randomUUID().toString())
                .customerId("cust-001")
                .product("Laptop")
                .quantity(1)
                .price(new BigDecimal("999.99"))
                .status(Order.OrderStatus.PENDING)
                .createdAt(LocalDateTime.now())
                .build();

        orderProducer.sendOrder(order);

        Order received = receivedOrders.poll(10, TimeUnit.SECONDS);

        assertThat(received).isNotNull();
        assertThat(received.getOrderId()).isEqualTo(order.getOrderId());
        assertThat(received.getCustomerId()).isEqualTo("cust-001");
        assertThat(received.getProduct()).isEqualTo("Laptop");
        assertThat(received.getStatus()).isEqualTo(Order.OrderStatus.PENDING);
    }

    @Test
    void shouldSendMultipleOrders() throws InterruptedException {
        int messageCount = 5;

        for (int i = 0; i < messageCount; i++) {
            Order order = Order.builder()
                    .orderId(UUID.randomUUID().toString())
                    .customerId("cust-" + i)
                    .product("Product-" + i)
                    .quantity(i + 1)
                    .price(new BigDecimal("10.00").multiply(BigDecimal.valueOf(i + 1)))
                    .status(Order.OrderStatus.PENDING)
                    .createdAt(LocalDateTime.now())
                    .build();
            orderProducer.sendOrder(order);
        }

        int received = 0;
        for (int i = 0; i < messageCount; i++) {
            Order order = receivedOrders.poll(10, TimeUnit.SECONDS);
            if (order != null) received++;
        }

        assertThat(received).isEqualTo(messageCount);
    }
}
