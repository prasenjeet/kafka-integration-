package com.example.kafka.consumer;

import com.example.kafka.model.Payment;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class PaymentConsumer {

    @KafkaListener(
            topics = "${kafka.topics.payments}",
            groupId = "${spring.kafka.consumer.group-id}",
            containerFactory = "paymentKafkaListenerContainerFactory"
    )
    public void consumePayment(
            @Payload Payment payment,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset,
            Acknowledgment acknowledgment) {

        log.info("Received payment: paymentId={}, orderId={}, status={}, partition={}, offset={}",
                payment.getPaymentId(), payment.getOrderId(), payment.getStatus(), partition, offset);

        try {
            processPayment(payment);
            acknowledgment.acknowledge();
        } catch (Exception e) {
            log.error("Failed to process payment: paymentId={}", payment.getPaymentId(), e);
        }
    }

    private void processPayment(Payment payment) {
        switch (payment.getStatus()) {
            case PENDING -> log.info("Payment pending: {}", payment.getPaymentId());
            case PROCESSING -> log.info("Payment processing: {}", payment.getPaymentId());
            case COMPLETED -> log.info("Payment completed for order: {}", payment.getOrderId());
            case FAILED -> log.warn("Payment failed for order: {}", payment.getOrderId());
            case REFUNDED -> log.info("Payment refunded for order: {}", payment.getOrderId());
        }
    }
}
