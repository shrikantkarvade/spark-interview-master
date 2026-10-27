# Module 5.2: Incremental Processing & Change Data Capture (CDC) Patterns

---

## 1. The Challenge of Stateful Updates in Big Data

Traditional data lakes built on immutable file formats (like raw Parquet or CSV) struggle with operational updates. When a source database (e.g., PostgreSQL, MySQL, or Oracle) experiences a row modification—such as an address change, a salary update, or a customer deletion—re-writing entire terabyte-scale partitions or performing full table reloads is computationally prohibitive and destroys pipeline SLAs.

To bridge transactional databases and analytical lakehouses, modern data engineering relies on **Change Data Capture (CDC)** and incremental processing frameworks.

---

## 2. Change Data Capture (CDC) Architecture

CDC mechanisms intercept database changes at the transaction log level (using tools like Debezium or AWS Database Migration Service) and stream row-level deltas (`INSERT`, `UPDATE`, `DELETE`) into messaging queues like Apache Kafka.

```
┌─────────────────────────┐     ┌─────────────────────────┐     ┌─────────────────────────┐
│     Source Database     │     │    Kafka / Event Hub    │     │   Spark Streaming ETL   │
│  (Postgres / MySQL)     │────►│ (Debezium JSON Payloads)│────►│  (Incremental Micro-    │
│  Transactional Changes  │     │   Row-level Deltas      │     │         batches)        │
└─────────────────────────┘     └─────────────────────────┘     └────────────┬────────────┘
                                                                             │
                                                                             ▼
                                                                ┌─────────────────────────┐
                                                                │  Target Lakehouse Table │
                                                                │  (Atomic MERGE INTO &   │
                                                                │   Change Data Feed)     │
                                                                └─────────────────────────┘

```

### The Ingestion Pipeline Flow:

1. **Capture:** Source database write operations are logged and emitted as stream events containing both old and new row values.
2. **Buffer:** Kafka partitions events by primary key to guarantee in-order delivery per entity.
3. **Process & Apply:** Spark Structured Streaming reads micro-batches and applies atomic state synchronization directly against the Lakehouse storage layer.

---

## 3. The `MERGE INTO` (Upsert) Engine

The cornerstone of modern lakehouse incremental processing is the SQL `MERGE` statement (often referred to as an "upsert").

### How Delta Lake Executes a Merge:

1. **Join Phase:** The engine joins the incoming source change dataset with the target Delta table using the primary key condition.
2. **Action Routing:** Rows are evaluated against conditional clauses (`WHEN MATCHED THEN UPDATE`, `WHEN NOT MATCHED THEN INSERT`, `WHEN NOT MATCHED BY SOURCE THEN DELETE`).
3. **Copy-on-Write / Merge-on-Read Optimization:**
* **Copy-on-Write (Default):** Rewrites only the Parquet data files that contain matching records, leaving untouched files intact.
* **Merge-on-Read:** Appends change updates into smaller delta files quickly, deferring compaction until background maintenance runs.



---

## 4. Slowly Changing Dimensions (SCD): Type 1 vs. Type 2

When managing dimensional data (e.g., customer profiles, employee records, product catalogs), data engineers must choose how historical updates are retained:

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                          SCD Type 1 (Latest State)                          │
│  • Overwrites existing records in place.                                    │
│  • Discards historical values; only current state matters.                 │
│  • Simple to implement via standard MERGE INTO updates.                     │
├─────────────────────────────────────────────────────────────────────────────┤
│                         SCD Type 2 (Historical Audit)                       │
│  • Preserves full historical audit trail.                                   │
│  • Closes existing records (sets end_date / is_current = false).            │
│  • Inserts a new active record version with start_date and is_current = true│
└─────────────────────────────────────────────────────────────────────────────┘

```

---

## 5. Change Data Feed (CDF) & Downstream Incremental Scans

Historically, downstream pipelines (such as Bronze-to-Silver or Silver-to-Gold medallion layers) had to perform full table scans or complex outer joins to detect what changed between runs.

**Change Data Feed (CDF)** solves this by allowing Delta tables to automatically track and emit row-level change metadata:

* **`insert`**: Newly added records.
* **`update_preimage`**: The state of the record before the update.
* **`update_postimage`**: The state of the record after the update.
* **`delete`**: Removed records.

Downstream consumers can query *only* the modified records between specific versions or timestamps, reducing ETL processing times by orders of magnitude.

---

## 6. Senior Interview Deep-Dive Q&A

### Q1: How do you handle out-of-order CDC events or duplicate updates in a streaming pipeline?

**Answer:**
Out-of-order events are a common hazard in Kafka-to-Lakehouse pipelines. To handle them deterministically:

1. **Sequence Numbering / Versioning:** Source databases emit an incrementing sequence number (e.g., database LSN or transaction timestamp).
2. **Conditional MERGE:** Configure the `MERGE INTO` update clause to apply changes *only* if the incoming event's sequence number is strictly greater than the target table's existing version (`WHEN MATCHED AND source.sequence_num > target.sequence_num THEN UPDATE`). This ensures idempotency and protects against late-arriving or duplicate packets.

### Q2: What is the performance trade-off between SCD Type 1 and SCD Type 2 in large-scale lakehouses?

**Answer:**

* **SCD Type 1** is computationally inexpensive because it modifies or replaces specific rows in place. However, it loses point-in-time historical context, making it unsuitable for financial auditing or historical reporting.
* **SCD Type 2** maintains complete historical accuracy but introduces significant write amplification and table bloat. Over time, tables grow rapidly, requiring aggressive partitioning, file compaction (`OPTIMIZE`), and Z-Ordering on entity IDs and effective date ranges to prevent query degradation.

---

### 🚀 Try It Out

Execute the experiment using Gradle:

```bash
./gradlew runCdcExperiment
```