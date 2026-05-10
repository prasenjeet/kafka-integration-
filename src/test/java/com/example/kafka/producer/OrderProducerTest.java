package com.example.kafka.producer;

import com.example.kafka.model.Order;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.test.util.ReflectionTestUtils;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class OrderProducerTest {

    @Mock
    private KafkaTemplate<String, Order> orderKafkaTemplate;

    @InjectMocks
    private OrderProducer orderProducer;

    @BeforeEach
    void setUp() {
        ReflectionTestUtils.setField(orderProducer, "ordersTopic", "orders-topic");
    }

    @Test
    void shouldSendOrderSuccessfully() {
        Order order = buildSampleOrder();
        CompletableFuture<SendResult<String, Order>> future = CompletableFuture.completedFuture(mock(SendResult.class));
        when(orderKafkaTemplate.send(eq("orders-topic"), eq(order.getOrderId()), eq(order))).thenReturn(future);

        orderProducer.sendOrder(order);

        verify(orderKafkaTemplate).send("orders-topic", order.getOrderId(), order);
    }

    @Test
    void shouldHandleProducerFailureGracefully() {
        Order order = buildSampleOrder();
        CompletableFuture<SendResult<String, Order>> failedFuture = new CompletableFuture<>();
        failedFuture.completeExceptionally(new RuntimeException("Broker unavailable"));
        when(orderKafkaTemplate.send(anyString(), anyString(), any(Order.class))).thenReturn(failedFuture);

        // Should not throw — failure is handled in the callback
        orderProducer.sendOrder(order);

        verify(orderKafkaTemplate).send(anyString(), anyString(), any(Order.class));
    }

    private Order buildSampleOrder() {
        return Order.builder()
                .orderId(UUID.randomUUID().toString())
                .customerId("cust-001")
                .product("Widget")
                .quantity(2)
                .price(new BigDecimal("29.99"))
                .status(Order.OrderStatus.PENDING)
                .createdAt(LocalDateTime.now())
                .build();
    }
}
