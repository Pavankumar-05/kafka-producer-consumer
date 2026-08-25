# Kafka with Spring Boot — Learning Project

This repository contains my hands-on implementation while learning **Apache Kafka with Spring Boot** by following the **Concepts and Coding Kafka playlist**.

The goal of this project is to understand Kafka concepts by implementing them practically rather than only studying the theory.

> This is a learning project based on the concepts demonstrated in the Concepts and Coding Kafka playlist. The code is my local implementation and experimentation while following along with the lessons.

---

## What I'm Learning

This project is being developed incrementally as I progress through the Kafka concepts.

The topics covered so far include:

* Kafka Producers
* Kafka Consumers
* Kafka Topics
* Kafka Partitions
* Kafka Message Keys
* Kafka Offsets
* Kafka Consumer Groups
* Multiple Topics
* Multiple Consumers
* Serialization
* Deserialization
* Custom Serialization
* JSON messages
* Producer acknowledgements
* Producer retries
* Idempotent producers
* Producer batching
* Compression
* Kafka log segments
* Inspecting Kafka records using Kafka command-line tools

---

## Project Structure

```text
kafka-producer-consumer/
│
├── kafka-producer-service/
│   ├── src/
│   ├── pom.xml
│   └── ...
│
├── kafka-consumer-service/
│   ├── src/
│   ├── pom.xml
│   └── ...
│
└── .gitignore
```

The project contains two Spring Boot applications.

### Kafka Producer Service

Responsible for:

* Receiving requests through REST APIs
* Creating Order and Payment events
* Publishing events to Kafka topics
* Experimenting with Kafka keys
* Observing partitions and offsets
* Configuring producer reliability and performance settings

### Kafka Consumer Service

Responsible for:

* Consuming events from Kafka topics
* Working with multiple Kafka topics
* Deserializing Kafka values
* Converting JSON messages into Java objects
* Experimenting with consumer groups

---

# Kafka Topics

The current implementation uses two topics:

```text
order-events
payment-events
```

### Order Event

Example:

```json
{
  "orderId": "O-1",
  "customerId": "CUST-2",
  "productId": "PROD-3",
  "quantity": 1,
  "totalAmount": 99.99,
  "status": "CREATED"
}
```

### Payment Event

Example:

```json
{
  "paymentId": "P-1021",
  "customerId": "CUST-2",
  "totalAmount": 99.99,
  "status": "CREATED"
}
```

---

# Basic Architecture

```text
                    REST Request
                         |
                         v
              +---------------------+
              | Kafka Producer      |
              | Spring Boot         |
              +----------+----------+
                         |
                         v
                  KafkaTemplate
                         |
              +----------+----------+
              |                     |
              v                     v
        order-events          payment-events
              |                     |
              +----------+----------+
                         |
                         v
              +---------------------+
              | Kafka Consumer      |
              | Spring Boot         |
              +----------+----------+
                         |
                         v
                  Java Objects
```

---

# Producer Configuration

The producer currently uses a String serializer for keys.

```properties
spring.kafka.producer.key-serializer=org.apache.kafka.common.serialization.StringSerializer
```

The value is serialized as JSON.

The producer configuration also demonstrates concepts such as:

```properties
spring.kafka.producer.acks=all
spring.kafka.producer.retries=3
spring.kafka.producer.properties.enable.idempotence=true
spring.kafka.producer.properties.batch.size=32768
spring.kafka.producer.properties.linger.ms=20
spring.kafka.producer.properties.compression.type=snappy
```

These settings are being explored as part of the Kafka learning process.

---

# Consumer Configuration

The consumer currently uses String deserialization:

```properties
spring.kafka.consumer.key-deserializer=org.apache.kafka.common.serialization.StringDeserializer

spring.kafka.consumer.value-deserializer=org.apache.kafka.common.serialization.StringDeserializer
```

The received JSON string is then converted into the appropriate Java object using Jackson.

Conceptually:

```text
Kafka
  |
  v
StringDeserializer
  |
  v
JSON String
  |
  v
Jackson ObjectMapper
  |
  +----> Order
  |
  +----> Payment
```

---

# Kafka Keys and Partitions

One of the concepts being explored is the relationship between a Kafka message key and its partition.

For example, when an Order is sent with a key:

```text
Order ID → Kafka Key
```

Kafka uses the key when determining the partition.

The producer response can be used to observe:

```text
Partition: 0
Offset: 0
```

This helps demonstrate how Kafka stores records within partitions.

---

# Running the Project

## Prerequisites

Install:

* Java 17+
* Maven
* Apache Kafka
* Git

Start the Kafka brokers before starting the Spring Boot applications.

The applications currently use local Kafka brokers such as:

```text
localhost:9092
localhost:9192
```

---

## Start Producer

```cmd
cd kafka-producer-service
mvn spring-boot:run
```

The producer runs on:

```text
http://localhost:8081
```

---

## Start Consumer

Open another terminal:

```cmd
cd kafka-consumer-service
mvn spring-boot:run
```

---

# Example API Request

### Create an Order

```http
POST http://localhost:8081/api/orders/with-key
```

Example request:

```json
{
  "orderId": "O-1",
  "customerId": "CUST-2",
  "productId": "PROD-3",
  "quantity": 1,
  "totalAmount": 99.99,
  "status": "CREATED"
}
```

Windows CMD:

```cmd
curl -X POST "http://localhost:8081/api/orders/with-key" -H "Content-Type: application/json" -d "{\"orderId\":\"O-1\",\"customerId\":\"CUST-2\",\"productId\":\"PROD-3\",\"quantity\":1,\"totalAmount\":99.99,\"status\":\"CREATED\"}"
```

---

# Inspecting Kafka Messages

Kafka's console consumer can be used to observe messages:

```cmd
kafka-console-consumer.bat --bootstrap-server localhost:9092 --topic order-events --from-beginning
```

To display keys, partitions and offsets:

```cmd
kafka-console-consumer.bat --bootstrap-server localhost:9092 --topic order-events --from-beginning --property print.key=true --property print.partition=true --property print.offset=true
```

---

# Inspecting Kafka Log Segments

As part of learning how Kafka stores records internally, Kafka log segment files can also be inspected using:

```cmd
kafka-dump-log.bat
```

Example:

```cmd
kafka-dump-log.bat --deep-iteration --print-data-log --files <KAFKA_HOME>\logs\<broker-log-directory>\payment-events-1\00000000000000000000.log
```

This helps explore the relationship between:

```text
Topic
  ↓
Partition
  ↓
Log Segment
  ↓
Offset
  ↓
Record
```

---

# Learning Approach

I'm building this project incrementally while following the Concepts and Coding Kafka playlist.

For each concept, I try to:

1. Understand the Kafka concept.
2. Implement the example in Spring Boot.
3. Run the Kafka brokers locally.
4. Produce messages.
5. Consume messages.
6. Inspect partitions and offsets.
7. Use Kafka command-line tools to understand what is happening internally.
8. Experiment by changing the configuration and observing the result.

This repository will therefore evolve as I progress through the Kafka topics.

---

# Current Learning Focus

The current implementation focuses on understanding:

```text
Producer
   ↓
Serializer
   ↓
Kafka Topic
   ↓
Partition
   ↓
Offset
   ↓
Consumer
   ↓
Deserializer
   ↓
Java Object
```

The project is intentionally kept as a learning environment so that Kafka concepts can be tested and observed individually.

---

# Reference

This project is being implemented while following the **Concepts and Coding Kafka playlist**.

YouTube channel:

**Concepts and Coding**

The repository is intended for learning and experimentation and is not an official implementation of the Concepts and Coding project.

---

## Author

**Pavan Kumar**

GitHub: [Pavankumar-05](https://github.com/Pavankumar-05)
