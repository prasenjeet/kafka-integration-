# Java Kafka Integration Sample

A comprehensive Spring Boot project demonstrating Apache Kafka integration patterns in Java.

## Project Structure

```
├── pom.xml
├── docker/
│   └── docker-compose.yml          # Kafka + ZooKeeper + Kafka UI
└── src/
    ├── main/java/com/example/kafka/
    │   ├── KafkaApplication.java
    │   ├── config/
    │   │   ├── KafkaTopicConfig.java      # Topic creation beans
    │   │   ├── KafkaProducerConfig.java   # Producer factory & templates
    │   │   └── KafkaConsumerConfig.java   # Consumer factory & listener containers
    │   ├── model/
    │   │   ├── Order.java
    │   │   └── Payment.java
    │   ├── producer/
    │   │   ├── OrderProducer.java         # Async/sync/header-based sending
    │   │   └── PaymentProducer.java
    │   ├── consumer/
    │   │   ├── OrderConsumer.java         # Single-record + batch listeners
    │   │   └── PaymentConsumer.java
    │   ├── service/
    │   │   └── OrderService.java          # Business logic wiring producers
    │   ├── admin/
    │   │   └── KafkaAdminService.java     # Topic & consumer group management
    │   ├── controller/
    │   │   └── OrderController.java       # REST endpoints to trigger events
    │   ├── serializer/
    │   │   ├── OrderSerializer.java       # Custom Jackson serializer
    │   │   └── OrderDeserializer.java     # Custom Jackson deserializer
    │   └── plain/
    │       ├── PlainKafkaProducer.java    # Raw kafka-clients producer (no Spring)
    │       └── PlainKafkaConsumer.java    # Raw kafka-clients consumer (no Spring)
    └── test/java/com/example/kafka/
        ├── KafkaIntegrationTest.java      # Embedded Kafka integration tests
        └── producer/
            └── OrderProducerTest.java     # Unit tests with Mockito
```

## Key Patterns Demonstrated

| Pattern | Location |
|---|---|
| Async send with callback | `OrderProducer.sendOrder()` |
| Sync (blocking) send | `PlainKafkaProducer.sendSync()` |
| Fire-and-forget | `PlainKafkaProducer.sendFireAndForget()` |
| Custom message headers | `OrderProducer.sendOrderWithHeaders()` |
| Send to specific partition | `OrderProducer.sendOrderToPartition()` |
| Manual offset commit | `OrderConsumer`, `PaymentConsumer` |
| Batch consumption | `OrderConsumer.consumeOrderBatch()` |
| Consumer rebalance listener | `PlainKafkaConsumer` constructor |
| Graceful consumer shutdown | `PlainKafkaConsumer.stop()` |
| Idempotent producer | `KafkaProducerConfig` (`enable.idempotence=true`) |
| Custom serializer/deserializer | `serializer/` package |
| Topic management via AdminClient | `KafkaAdminService` |
| Embedded Kafka for tests | `KafkaIntegrationTest` |

## Prerequisites

- Java 17+
- Maven 3.8+
- Docker & Docker Compose

## Quick Start

### 1. Start Kafka

```bash
cd docker
docker-compose up -d
```

Kafka UI is available at http://localhost:8090

### 2. Build and Run

```bash
mvn clean package -DskipTests
mvn spring-boot:run
```

### 3. Publish an Order via REST

```bash
curl -X POST http://localhost:8080/api/orders \
  -H "Content-Type: application/json" \
  -d '{
    "customerId": "cust-001",
    "product": "Laptop",
    "quantity": 1,
    "price": 999.99
  }'
```

### 4. Confirm an Order

```bash
curl -X PUT "http://localhost:8080/api/orders/{orderId}/confirm?customerId=cust-001"
```

### 5. Run Tests

```bash
mvn test
```

Tests use an **embedded Kafka broker** — no external Kafka required.

## Configuration

Key settings in `src/main/resources/application.yml`:

```yaml
spring.kafka.bootstrap-servers: localhost:9092
kafka.topics.orders: orders-topic
kafka.topics.payments: payments-topic
kafka.partitions: 3
kafka.replication-factor: 1
```

## Plain Kafka Client Examples

The `plain/` package shows identical patterns without Spring, using the raw `kafka-clients` API:

```bash
# Producer demo
mvn exec:java -Dexec.mainClass="com.example.kafka.plain.PlainKafkaProducer"

# Consumer demo (Ctrl+C to stop)
mvn exec:java -Dexec.mainClass="com.example.kafka.plain.PlainKafkaConsumer"
```

## Topics Created on Startup

| Topic | Partitions | Purpose |
|---|---|---|
| `orders-topic` | 3 | Order lifecycle events |
| `payments-topic` | 3 | Payment processing events |
| `notifications-topic` | 1 | User notifications |