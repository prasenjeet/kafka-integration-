package com.example.kafka.serializer;

import com.example.kafka.model.Order;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serializer;

@Slf4j
public class OrderSerializer implements Serializer<Order> {

    private final ObjectMapper objectMapper;

    public OrderSerializer() {
        this.objectMapper = new ObjectMapper().registerModule(new JavaTimeModule());
    }

    @Override
    public byte[] serialize(String topic, Order order) {
        if (order == null) return null;
        try {
            return objectMapper.writeValueAsBytes(order);
        } catch (JsonProcessingException e) {
            log.error("Failed to serialize Order: {}", order.getOrderId(), e);
            throw new RuntimeException("Serialization failed for Order", e);
        }
    }
}
