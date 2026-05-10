package com.example.kafka.controller;

import com.example.kafka.model.Order;
import com.example.kafka.service.OrderService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.math.BigDecimal;
import java.util.Map;

@Slf4j
@RestController
@RequestMapping("/api/orders")
@RequiredArgsConstructor
public class OrderController {

    private final OrderService orderService;

    @PostMapping
    public ResponseEntity<Order> createOrder(@RequestBody CreateOrderRequest request) {
        log.info("REST: creating order for customer={}", request.customerId());
        Order order = orderService.createOrder(
                request.customerId(),
                request.product(),
                request.quantity(),
                request.price()
        );
        return ResponseEntity.ok(order);
    }

    @PutMapping("/{orderId}/confirm")
    public ResponseEntity<Map<String, String>> confirmOrder(
            @PathVariable String orderId,
            @RequestParam String customerId) {
        orderService.confirmOrder(orderId, customerId);
        return ResponseEntity.ok(Map.of("status", "confirmed", "orderId", orderId));
    }

    public record CreateOrderRequest(
            String customerId,
            String product,
            int quantity,
            BigDecimal price
    ) {}
}
