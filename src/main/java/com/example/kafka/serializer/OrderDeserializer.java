package com.example.kafka.serializer;

import com.example.kafka.model.Order;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Deserializer;

@Slf4j
public class OrderDeserializer implements Deserializer<Order> {

    private final ObjectMapper objectMapper;

    public OrderDeserializer() {
        this.objectMapper = new ObjectMapper().registerModule(new JavaTimeModule());
    }

    @Override
    public Order deserialize(String topic, byte[] data) {
        if (data == null) return null;
        try {
            return objectMapper.readValue(data, Order.class);
        } catch (Exception e) {
            log.error("Failed to deserialize Order from topic: {}", topic, e);
            throw new RuntimeException("Deserialization failed for Order", e);
        }
    }
}
