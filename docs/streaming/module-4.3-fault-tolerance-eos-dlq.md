# Module 4.3 — Fault Tolerance, Checkpointing, Exactly-Once Semantics, & Dead Letter Queue (DLQ) Patterns

## 🧠 Core Architectural Concepts

### 1. Fault Tolerance Architecture & Checkpoint Logging

Spark Structured Streaming achieves fault tolerance through persistent logging in a checkpoint directory. Every micro-batch writes deterministic metadata logs:

* **Offset Log (`/offsets/`)**: Records the exact ending offsets read from sources per micro-batch.
* **Commit Log (`/commits/`)**: Records which micro-batches have been successfully committed to the sink.
* **State Store Directory (`/state/`)**: Preserves incremental snapshots of stateful operator data.

### 2. End-to-End Exactly-Once Semantics (EOS)

* Spark guarantees **exactly-once processing within its execution engine** via Write-Ahead Logs (WAL) and idempotent source offset tracking.
* **The Sink Constraint**: True end-to-end EOS depends entirely on the downstream sink capability:
* *At-Least-Once Sinks*: Console or standard file sinks (may duplicate records on retry).
* *Idempotent Sinks*: Upsert-capable databases using primary keys.
* *Transactional Sinks*: Delta Lake ACID logs, which guarantee atomic commits so retried micro-batches do not write duplicate data.



### 3. Poison Pills & Dead Letter Queue (DLQ) Pattern

When parsing raw data streams (such as JSON strings), malformed syntax, schema evolution mismatches, or corrupted payloads can throw runtime parsing exceptions that crash the streaming execution thread.

* **Solution**: Using Spark's `PERMISSIVE` mode along with the built-in `_corrupt_record` metadata column intercepts unparseable records. The stream splits cleanly into:
1. *Main Pipeline*: Processes healthy records.
2. *Dead Letter Queue (DLQ)*: Routes poisoned payloads to a separate audit sink for inspection without interrupting pipeline uptime.

---

## ❓ Top Senior Interview Questions & Strategic Answers

### Q1: Can Spark guarantee true end-to-end exactly-once semantics out-of-the-box when writing to a Kafka sink?

**Answer:**
No. While Spark guarantees exactly-once processing *up to the output operation*, writing to a standard Kafka producer sink is an at-least-once operation because network retries can duplicate messages if a failure occurs mid-commit. To achieve true end-to-end exactly-once semantics with Kafka, Spark must use **Kafka's transactional producer capabilities** (`spark.kafka.producer.common.config` with transactional ID settings) or write to an atomic transactional sink like **Delta Lake**, which tracks batch IDs in its transaction log to ignore duplicate writes.

### Q2: How do you handle schema drift or malformed records in a streaming pipeline without crashing the job?

**Answer:**
You implement the **Dead Letter Queue (DLQ) pattern**. Instead of parsing JSON with strict enforcement (`FAILFAST` mode), use `PERMISSIVE` mode and define an explicit `_corrupt_record string` column in your schema. Spark will catch unparseable rows and place the raw text into `_corrupt_record`. You then apply a filter: route rows where `_corrupt_record IS NOT NULL` to a dedicated DLQ sink (like cloud object storage or an error topic), and filter them out of your primary processing pipeline.

### 🚀 Try It Out

Execute the experiment using Gradle:

```bash
./gradlew runFaultToleranceExperiment
```