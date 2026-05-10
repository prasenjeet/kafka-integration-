package com.example.kafka.producer;

import com.example.kafka.model.Payment;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;

@Slf4j
@Service
@RequiredArgsConstructor
public class PaymentProducer {

    private final KafkaTemplate<String, Payment> paymentKafkaTemplate;

    @Value("${kafka.topics.payments}")
    private String paymentsTopic;

    public CompletableFuture<SendResult<String, Payment>> sendPayment(Payment payment) {
        log.debug("Sending payment: paymentId={}, orderId={}", payment.getPaymentId(), payment.getOrderId());

        CompletableFuture<SendResult<String, Payment>> future =
                paymentKafkaTemplate.send(paymentsTopic, payment.getOrderId(), payment);

        future.whenComplete((result, ex) -> {
            if (ex == null) {
                log.info("Payment sent: paymentId={}, partition={}, offset={}",
                        payment.getPaymentId(),
                        result.getRecordMetadata().partition(),
                        result.getRecordMetadata().offset());
            } else {
                log.error("Payment send failed: paymentId={}", payment.getPaymentId(), ex);
            }
        });

        return future;
    }
}
