# Kafka Message Ordering Demo

This repository is a technical walkthrough of Kafka's per-key ordering guarantees and the stale database writes that can occur when concurrent consumers process unkeyed messages. It demonstrates both behaviors end to end and verifies the results against MySQL.

## Concepts Demonstrated

- How Kafka message keys map records to partitions and preserve per-entity order.
- Why unkeyed messages processed concurrently can produce out-of-order writes.
- How version-guarded upserts protect stored state from stale events.
- How to verify consumer correctness by comparing database state with ground truth.

---

## The Problem

In distributed systems, services often consume Kafka messages concurrently across multiple threads/partitions. When messages carry no key, Kafka distributes them round-robin — there is no guarantee that updates for the same entity arrive in order or land on the same thread. This causes **race conditions**:

> Thread A processes version 5, Thread B processes version 3 (delayed) — Thread B writes last → version 3 silently overwrites version 5 in the database.

This is called **out-of-order write corruption** and it is hard to detect because no error is thrown — the data is just quietly wrong.

---

## The Solution

When every message is **keyed by `entityId`**, Kafka's partitioner hashes the key to always route the same entity to the same partition. Since a partition is consumed by exactly one thread at a time, updates for the same entity are processed **strictly in order** — no race conditions possible.

---

## Architecture

### End-to-End Pipeline

```mermaid
flowchart TD
    subgraph REST["REST API - port 8085"]
        DC["DemoController"]
        VC["VerificationController"]
    end

    subgraph Services["Services"]
        SS["SeederService"]
        PS["OrderSyncPublishService"]
    end

    subgraph Job["Scheduler"]
        JOB["OrderSyncDiscoveryJob"]
    end

    subgraph DB_A["MySQL - Ground Truth"]
        SD[("seed_data")]
    end

    DC -->|seed| SS
    DC -->|run or run-all| PS
    SS -->|save 5k records| SD
    PS -->|increment versions| SD
    JOB -->|delegate| PS

    subgraph Producers["Kafka Producers"]
        NP["Naive producer<br/>Key: null"]
        OP["Ordered producer<br/>Key: entityId"]
    end

    PS --> NP
    PS --> OP

    subgraph KafkaBroker["Kafka Broker - localhost 9092"]
        subgraph T1["naviceOrderSync - 3 partitions"]
            NA["P0 P1 P2"]
        end
        subgraph T2["orderSync - 3 partitions"]
            OA["P0 P1 P2"]
        end
    end

    NP -->|round robin| NA
    OP -->|hash by entityId| OA

    subgraph NaiveConsumer["Naive Consumer - concurrency 3"]
        NC["Deserialize message"]
        NW["Blind upsert<br/>Last writer wins"]
    end

    subgraph OrderedConsumer["Ordered Consumer - concurrency 3"]
        OC["Deserialize message"]
        OW["Guarded upsert<br/>Only newer version"]
    end

    NA --> NC
    OA --> OC

    NC --> NW
    OC --> OW

    subgraph DB_B["MySQL - State B"]
        NS[("naive_state")]
    end

    subgraph DB_C["MySQL - State C"]
        OS[("ordered_state")]
    end

    NW --> NS
    OW --> OS

    VC -->|verify A against B and C| SD
    VC -->|reads| NS
    VC -->|reads| OS
```

---

### Class Hierarchy

```mermaid
classDiagram
    class ProducerStrategy {
        <<interface>>
        +send(topicName, payload)
    }

    class OrderSyncProducerStrategy {
        <<interface>>
    }

    class NaiveOrderSyncProducer {
        -KafkaTemplate kafkaTemplate
        +send(topicName, message)
        keyIsNull
    }

    class OrderedOrderSyncProducer {
        -KafkaTemplate kafkaTemplate
        +send(topicName, message)
        keyIsEntityId
    }

    ProducerStrategy <|-- OrderSyncProducerStrategy
    OrderSyncProducerStrategy <|.. NaiveOrderSyncProducer
    OrderSyncProducerStrategy <|.. OrderedOrderSyncProducer

    class AbstractKafkaConsumer {
        #ObjectMapper objectMapper
        #consumeRaw(rawJson, key, partition, offset)
        #process(payload, key, partition, offset)
    }

    class AbstractOrderSyncConsumer {
        payloadTypeIsOrderSyncMessage
    }

    class NaiveOrderSyncConsumer {
        -NaiveStateRepository repo
        +listen()
        #process blindUpsert
    }

    class OrderedOrderSyncConsumer {
        -OrderedStateRepository repo
        +listen()
        #process guardedUpsert
    }

    AbstractKafkaConsumer <|-- AbstractOrderSyncConsumer
    AbstractOrderSyncConsumer <|-- NaiveOrderSyncConsumer
    AbstractOrderSyncConsumer <|-- OrderedOrderSyncConsumer

    class OrderSyncMessage {
        +String entityId
        +long version
        +String eventType
        +Object data
    }

    class SeedData {
        +String entityId PK
        +long version
        +String eventType
        +String data
        +Instant updatedAt
    }

    class NaiveState {
        +String entityId PK
        +long version
        +String eventType
        +String data
        +Instant updatedAt
    }

    class OrderedState {
        +String entityId PK
        +long version
        +String eventType
        +String data
        +Instant updatedAt
    }

    class SeederService {
        -SeedDataRepository repo
        +seed() String
    }

    class OrderSyncPublishService {
        -SeedDataRepository seedRepo
        -NaiveOrderSyncProducer naiveProducer
        -OrderedOrderSyncProducer orderedProducer
        +runOneRound() String
        +runAllRounds() String
        +resetCompletedFlag()
    }

    class OrderSyncDiscoveryJob {
        -OrderSyncPublishService publishService
        +discovery() @Scheduled
    }

    SeederService --> SeedData
    OrderSyncPublishService --> SeedData
    OrderSyncPublishService --> NaiveOrderSyncProducer
    OrderSyncPublishService --> OrderedOrderSyncProducer
    OrderSyncDiscoveryJob --> OrderSyncPublishService
    NaiveOrderSyncConsumer --> NaiveState
    OrderedOrderSyncConsumer --> OrderedState
    NaiveOrderSyncProducer ..> OrderSyncMessage
    OrderedOrderSyncProducer ..> OrderSyncMessage
```

---

### Database Schema

```mermaid
erDiagram
    seed_data {
        VARCHAR entityId PK
        BIGINT version
        VARCHAR eventType
        TEXT data
        TIMESTAMP updatedAt
    }

    naive_state {
        VARCHAR entityId PK
        BIGINT version
        VARCHAR eventType
        TEXT data
        TIMESTAMP updatedAt
    }

    ordered_state {
        VARCHAR entityId PK
        BIGINT version
        VARCHAR eventType
        TEXT data
        TIMESTAMP updatedAt
    }

    seed_data ||--o| naive_state : compares
    seed_data ||--o| ordered_state : compares
```

---

## Data Flow

### 1. Ground Truth — State A (`seed_data` table)
- `POST /demo/seed` inserts **5,000 unique `OrderSyncMessage` records** at `version = 1`.
- This is the source of truth. After each publish round the job increments the version here too, so A always reflects the expected final state.

### 2. Producer Rounds
- `POST /demo/run` triggers one round: all 5,000 entities get their version incremented by 1 and published to **both** topics.
- 4 rounds total → final expected version = **5**.
- All 5,000 messages for version N are published before moving to version N+1.

### 3. Consumers Write to DB

| Consumer | Topic | Key | Concurrency | DB Write | Result |
|---|---|---|---|---|---|
| `NaiveOrderSyncConsumer` | `naviceOrderSync` | none (null) | 3 threads | `blindUpsert` — no version check | State B — **corrupted** |
| `OrderedOrderSyncConsumer` | `orderSync` | `entityId` | 3 threads | `guardedUpsert` — only if `newVersion > stored` | State C — **correct** |

### 4. Verification
`GET /verify` compares A vs B and A vs C:

```json
{
  "groundTruth":  { "totalEntities": 5000, "expectedVersion": 5 },
  "naiveState":   { "totalProcessed": 5000, "correct": 3821, "corrupted": 1179, "missing": 0, "corruptionRate": "23.58%" },
  "orderedState": { "totalProcessed": 5000, "correct": 5000, "corrupted": 0,    "missing": 0, "corruptionRate": "0.00%" }
}
```

### 5. Proof from the Database

The screenshot below shows the SQL query used to count records whose version is behind the final ground-truth version:

```sql
SELECT count(*) as naive_count
FROM naive_state ns
WHERE ns.version <> 5
UNION
SELECT count(*) as ordered_count
FROM ordered_state ns
WHERE ns.version <> 5;
```

![Naive and ordered state verification screenshot](<Screenshot 2026-08-15 at 9.17.03 PM.png>)

The result shows **1,243 stale rows** in `naive_state` and **0 stale rows** in `ordered_state`. This proves that the unkeyed Kafka topic experiences out-of-order/back-pressure-style consumer writes, while keying by `entityId` keeps updates for the same entity ordered and prevents stale versions from being written.

---

## Project Structure

```
src/main/java/com/example/kafkamessageordering/
├── config/
│   ├── KafkaConfig.java               # Producer + Consumer factories
│   ├── KafkaTopicConfig.java          # Topic creation (naviceOrderSync, orderSync)
│   └── OrderSyncKafkaProperties.java  # Typed config properties
├── controller/
│   ├── DemoController.java            # POST /demo/seed, /run, /run-all, /reset
│   └── VerificationController.java    # GET /verify
├── entity/
│   ├── SeedData.java                  # Table: seed_data  (State A)
│   ├── NaiveState.java                # Table: naive_state (State B)
│   └── OrderedState.java              # Table: ordered_state (State C)
├── job/
│   └── OrderSyncDiscoveryJob.java     # Scheduled job (manual or auto)
├── kafka/
│   ├── consumer/
│   │   ├── AbstractKafkaConsumer.java
│   │   ├── AbstractOrderSyncConsumer.java
│   │   ├── NaiveOrderSyncConsumer.java   # Writes State B (blind upsert)
│   │   └── OrderedOrderSyncConsumer.java # Writes State C (guarded upsert)
│   └── producer/
│       ├── NaiveOrderSyncProducer.java   # Sends with null key
│       └── OrderedOrderSyncProducer.java # Sends with entityId as key
├── model/
│   └── OrderSyncMessage.java
├── repository/
│   ├── SeedDataRepository.java
│   ├── NaiveStateRepository.java      # blindUpsert()
│   └── OrderedStateRepository.java    # guardedUpsert()
└── service/
    ├── SeederService.java             # Generates 5,000 seed records
    └── OrderSyncPublishService.java   # Core publish logic (used by job + controller)
```

---

## Prerequisites

- Java 21
- Maven
- Kafka running on `localhost:9092`
- MySQL running on `localhost:3306`

### MySQL Setup (one-time)

```sql
CREATE DATABASE demo_db;
```

Hibernate auto-creates the tables (`seed_data`, `naive_state`, `ordered_state`) on first startup.

---

## Running the App

```bash
./mvnw spring-boot:run
```

The scheduler is **disabled by default** (`ordersync.scheduler-enabled: false`).  
Use the REST endpoints below for manual control during the demo.

---

## Demo Walkthrough

```bash
# Step 1 — seed ground truth (5,000 entities at version=1)
curl -X POST http://localhost:8085/demo/seed

# Step 2a — run all 4 rounds at once (version 1→2→3→4→5)
curl -X POST http://localhost:8085/demo/run-all

# Step 2b — OR run one round at a time (good for live demo)
curl -X POST http://localhost:8085/demo/run   # version 2
curl -X POST http://localhost:8085/demo/run   # version 3
curl -X POST http://localhost:8085/demo/run   # version 4
curl -X POST http://localhost:8085/demo/run   # version 5

# Step 3 — see the corruption report
curl http://localhost:8085/verify

# Step 4 — reset consumer states to demo again (keeps seed entities)
curl -X POST http://localhost:8085/demo/reset
```

---

## Key Insight

The corruption in `naive_state` is not a bug in the consumer code — it is the **inevitable consequence of concurrent threads processing unordered messages**. The naive consumer does everything "correctly" — it just has no way to know it's receiving an older version after a newer one already landed.

Keyed partitioning removes the non-determinism entirely. The fix is not in the consumer logic — it is in how messages are routed to partitions.
