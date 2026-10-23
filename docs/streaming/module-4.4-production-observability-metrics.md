# Module 4.4 — Production Observability, Metrics Collection, & StreamingQueryListener

## 🧠 Core Architectural Concepts

### 1. StreamingQueryListener Telemetry Hook

Production monitoring requires programmatic visibility into runtime health. `StreamingQueryListener` is an asynchronous event listener interface provided by Spark that triggers callbacks for:

* `onQueryStarted`: Emits application startup metadata and query IDs.
* `onQueryProgress`: Emits granular JSON-formatted progress metrics after every completed micro-batch.
* `onQueryTerminated`: Captures graceful shutdowns or runtime exception stack traces.

### 2. Key Progress Metrics & Lag Detection

The `StreamingQueryProgress` object exposes vital telemetry for cluster health checks:

* `numInputRows`: Total records ingested in the current batch.
* `inputRowsPerSecond` vs. `processedRowsPerSecond`: If ingestion rate permanently outpaces processing rate, the pipeline is falling behind.
* **Backpressure Warnings**: Spark logs `Current batch is falling behind` when batch execution duration exceeds the configured trigger interval.

### 3. Granular Latency Breakdown (`durationMs`)

To pinpoint performance bottlenecks, senior engineers analyze individual phase durations:

* `triggerExecution`: Total wall-clock time taken by the micro-batch cycle.
* `queryPlanning`: Time spent in the Catalyst Optimizer generating physical plans.
* `getBatch` / `addBatch`: Time spent reading from sources and writing to sinks/state stores.
* `walCommit` / `commitOffsets`: Metadata durability commit latency.

---

## ❓ Top Senior Interview Questions & Strategic Answers

### Q1: You notice `durationMs.addBatch` is spiking exponentially over time while throughput drops. What does this indicate, and how do you diagnose it?

**Answer:**

* **Diagnosis**: This is a classic symptom of **unbounded state growth or state store inefficiency**. As state grows (e.g., lack of proper watermarking or missing state cleanup in `mapGroupsWithState`), reading and writing state snapshots or RocksDB blocks during `addBatch` becomes heavier every micro-batch.
* **Resolution**: Inspect the UI/Metrics for state store size and number of keys. Ensure watermarks are correctly configured and propagated, verify that old state eviction is occurring, and consider tuning RocksDB configuration parameters (such as block cache size and compaction settings).

### Q2: How would you export Spark Structured Streaming metrics into an enterprise monitoring tool like Prometheus or Datadog?

**Answer:**
Spark supports pluggable metrics reporting via **Dropwizard Metrics**. You can configure Spark's `metrics.properties` file to enable the JMX sink or Prometheus servlet sink. Alternatively, you can implement a custom `StreamingQueryListener` that intercepts `onQueryProgress` events and publishes JSON metrics to an external messaging bus (like Kafka), a logging daemon, or a time-series database.

### 🚀 Try It Out

Execute the experiment using Gradle:

```bash
./gradlew runObservabilityExperiment
```