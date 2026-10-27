# Module 5.6: Structured Streaming with Delta Lake (Source & Sink Patterns)

## 1. Introduction to Streaming Lakehouses

Traditional data pipelines often maintain separate architectures for batch processing (e.g., nightly ETL jobs) and streaming ingestion (e.g., Kafka consumers). The Lakehouse paradigm unifies these workloads by combining **Apache Spark Structured Streaming** with **Delta Lake**, enabling continuous, real-time data ingestion while retaining full ACID guarantees, time travel, and schema enforcement.

```
┌─────────────────────────────────┐     ┌─────────────────────────────────┐
│       Streaming Sources         │     │   Structured Streaming Engine   │
│  (Kafka, File Stream, Rate Gen) │────►│  • Watermarking & Event Time    │
└─────────────────────────────────┘     │  • Micro-batch / Continuous     │
                                        └──────────────┬──────────────────┘
                                                       │
                                                       ▼
                                        ┌─────────────────────────────────┐
                                        │        Delta Table Sink         │
                                        │  • Exactly-once Semantics       │
                                        │  • Checkpoint-backed State      │
                                        │  • Auto-compaction & Z-ordering │
                                        └─────────────────────────────────┘

```

---

## 2. Delta Lake as a Streaming Sink

When writing streaming data into a data lake, common challenges include partial file writes, phantom reads, and small file proliferation. Delta Lake solves these issues by acting as a transactional streaming sink.

* **Atomic Micro-Batch Commits:** Every streaming micro-batch writes data files and commits an atomic JSON entry to the `_delta_log`. Readers never see partial or half-written batches.
* **Append and Update Output Modes:** Delta sinks support `append` mode for standard telemetry/log streams, as well as `complete` mode for aggregated metrics. When combined with `MERGE INTO`, streaming sources can perform real-time upserts.
* **Auto-Optimization:** Streaming micro-batches naturally generate thousands of small files. Integrating background table maintenance (`OPTIMIZE` and `VACUUM`) ensures storage layout remains performant over continuous streaming lifecycles.

---

## 3. Delta Lake as a Streaming Source (Delta-to-Delta Pipelines)

One of Delta Lake's most powerful features is its ability to act as a **streaming source** (`spark.readStream.format("delta").load(path)`).

* **Log-Based Reading:** Instead of scanning directories or listing cloud object storage objects, the streaming engine reads the Delta transaction log (`_delta_log`). It tracks newly committed files automatically, making incremental reads extremely fast and lightweight.
* **Medallion Streaming Integration:** This powers real-time multi-hop pipelines. Bronze tables stream new data continuously into Silver ingestion jobs, which in turn stream clean records into Gold aggregation tables with minimal latency.

---

## 4. Checkpointing, Fault Tolerance, and Exactly-Once Semantics

In distributed streaming architectures, network failures, node crashes, or cluster restarts are inevitable. Spark and Delta guarantee reliability through rigorous state management:

* **Checkpoint Directories:** Spark Structured Streaming writes offset metadata, commit logs, and state store data into a designated `checkpointLocation`.
* **End-to-End Exactly-Once Semantics:** By combining Delta's idempotent transaction log with Spark's offset tracking checkpoints, the platform guarantees that records are processed and committed exactly once, even if micro-batches are re-attempted following a failure.

---

## 5. Senior Interview Deep-Dive Q&A

### Q1: How does Delta Lake as a streaming source differ from reading files via a standard file stream source?

**Answer:**
Standard file stream sources (`format("file")` or `format("parquet")`) rely on directory listing polls. As file counts grow into millions, directory listing operations cause severe metadata bottlenecks and slow down streaming micro-batch starts. In contrast, Delta Lake as a streaming source reads directly from the immutable transaction log (`_delta_log`). It instantly identifies newly added files without performing expensive object store listings, scaling efficiently regardless of total historical table size.

### Q2: What happens if a streaming job crashes and you change the query logic before restarting from the same checkpoint?

**Answer:**
Changing the streaming transformation logic while reusing an existing checkpoint directory can cause severe state corruption or analysis exceptions. Spark's checkpoint store retains serialized execution plans, offsets, and aggregation states. If you alter the schema or logical plan incompatibly, the stream will fail upon restart. For structural changes, engineers must either migrate the checkpoint state or start the stream from a fresh checkpoint directory with a full or snapshot-backed replay.

---

### 🚀 Try It Out

Execute the experiment using Gradle:

```bash
./gradlew runStreamingLakeHouseExperiment
```