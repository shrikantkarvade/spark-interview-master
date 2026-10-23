# Module 4.1 — Streaming Architecture, Triggers, Backpressure, & Observability

## 🧠 Core Architectural Concepts

### 1. Processing Engines: Micro-batch vs. Continuous
* **Micro-Batch Engine (Default)**:
  * Uses the Catalyst Optimizer and Tungsten Execution Engine to treat streaming data as discretized mini-batches.
  * Achieves high throughput with end-to-end latency typically between **100ms - 500ms**.
  * Supports all stateful transformations, aggregations, and complex joins.
* **Continuous Processing (Experimental)**:
  * Low-latency engine capable of sub-millisecond latency (`~1ms`).
  * Uses long-running epoch-based tasks instead of recurring query launches.
  * *Trade-off*: Limited support for complex stateful operations, watermarks, and windowing.

---

### 2. Trigger Policies Comparison

| Trigger Type | Code Pattern | Use Case / Behavior |
| :--- | :--- | :--- |
| **Default (Unspecified)** | `.trigger(Trigger.ProcessingTime("0 seconds"))` | Processes micro-batches as fast as possible sequentially. |
| **Fixed Interval** | `.trigger(Trigger.ProcessingTime("10 seconds"))` | Executes batches at precise wall-clock intervals. |
| **AvailableNow** | `.trigger(Trigger.AvailableNow())` | Processes all existing available data in incremental micro-batches, then gracefully shuts down. Ideal for cost-optimized periodic jobs (e.g., hourly streaming pipelines). |
| **Continuous** | `.trigger(Trigger.Continuous("1 second"))` | Continuous processing mode for sub-millisecond real-time requirements. |

---

### 3. Backpressure & Rate Control Strategies
Without rate limits, a stream recovering from downtime will pull massive bursts of data, causing **Driver/Executor OOMs** or **Disk Spills**.

* **Kafka Source Limits**:
```scala
spark.sql.streaming.kafka.maxRatePerPartition = 5000
```

Caps the max records fetched per partition per micro-batch.

* **File/Directory Source Limits**:
```scala
spark.sql.streaming.maxFilesPerTrigger = 10
spark.sql.streaming.maxBytesPerTrigger = 1073741824 // 1 GB
```

---

### 4. Production Telemetry: `StreamingQueryListener`

Attaching a custom `StreamingQueryListener` exposes continuous runtime metrics:

* **`inputRowsPerSecond` vs. `processedRowsPerSecond**`:
* If `inputRowsPerSecond > processedRowsPerSecond` persistently, the cluster is under-provisioned or experiencing processing lag.


* **`durationMs.triggerExecution`**:
* Tracks how long each micro-batch takes to execute.

---

## ❓ Top Senior Interview Questions & Strategic Answers

### Q1: How do you prevent a Structured Streaming job from crashing with an OOM when restarting after 24 hours of downtime?

**Answer:**
When a streaming pipeline restarts after extended downtime, backlog accumulates at the source (e.g., millions of unread Kafka messages or thousands of unparsed files). If left uncapped, the driver will construct a single massive micro-batch, causing memory exhaustion.

**Solution:**

1. Configure explicit rate limits:
* For Kafka: Set `spark.streaming.kafka.maxRatePerPartition` (e.g., `2000`).
* For File Sources: Set `spark.sql.streaming.maxFilesPerTrigger` or `maxBytesPerTrigger`.


2. This forces Spark to divide the accumulated backlog into manageable, incremental micro-batches until the pipeline catches up to the real-time head.

### Q2: What is the difference between `Trigger.Once` and `Trigger.AvailableNow`?

**Answer:**

* `Trigger.Once` processes *all* available data in a **single micro-batch**. If 10 TB of backlog exists, it loads 10 TB into one batch, risking severe OOMs or executor timeout failures.
* `Trigger.AvailableNow` processes all available data across **multiple incremental micro-batches** respecting configured rate limits (e.g., `maxFilesPerTrigger` or `maxRatePerPartition`), and then gracefully terminates the stream once caught up.

### 🚀 Try It Out

Execute the experiment using Gradle:

```bash
./gradlew runStreamingArchExperiment
```