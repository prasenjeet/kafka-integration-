package com.example.kafka.service;

import com.example.kafka.model.Order;
import com.example.kafka.model.Payment;
import com.example.kafka.producer.OrderProducer;
import com.example.kafka.producer.PaymentProducer;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.UUID;

@Slf4j
@Service
@RequiredArgsConstructor
public class OrderService {

    private final OrderProducer orderProducer;
    private final PaymentProducer paymentProducer;

    public Order createOrder(String customerId, String product, int quantity, BigDecimal price) {
        Order order = Order.builder()
                .orderId(UUID.randomUUID().toString())
                .customerId(customerId)
                .product(product)
                .quantity(quantity)
                .price(price)
                .status(Order.OrderStatus.PENDING)
                .createdAt(LocalDateTime.now())
                .build();

        orderProducer.sendOrder(order);
        log.info("Order created and published: orderId={}", order.getOrderId());
        return order;
    }

    public void confirmOrder(String orderId, String customerId) {
        Order order = Order.builder()
                .orderId(orderId)
                .customerId(customerId)
                .status(Order.OrderStatus.CONFIRMED)
                .createdAt(LocalDateTime.now())
                .build();

        orderProducer.sendOrderWithHeaders(order, "order-service");
        log.info("Order confirmed: orderId={}", orderId);
    }

    public void processPaymentForOrder(Order order) {
        Payment payment = Payment.builder()
                .paymentId(UUID.randomUUID().toString())
                .orderId(order.getOrderId())
                .customerId(order.getCustomerId())
                .amount(order.getPrice().multiply(BigDecimal.valueOf(order.getQuantity())))
                .method(Payment.PaymentMethod.CREDIT_CARD)
                .status(Payment.PaymentStatus.PENDING)
                .processedAt(LocalDateTime.now())
                .build();

        paymentProducer.sendPayment(payment);
        log.info("Payment initiated for order: orderId={}, paymentId={}", order.getOrderId(), payment.getPaymentId());
    }
}
