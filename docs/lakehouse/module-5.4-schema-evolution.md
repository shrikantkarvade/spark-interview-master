# Module 5.4: Schema Evolution & Data Compatibility

---

## 1. The Challenge of Schema Drift in Big Data

In traditional relational databases, schemas are strictly enforced at the storage layer via DDL constraints. However, distributed data lakes ingest high-velocity data from diverse upstream sources (microservices, mobile apps, third-party APIs) where schemas are constantly evolving.

**Schema Drift** occurs when upstream data structures change unexpectedly—such as engineers adding new telemetry fields, renaming columns, or changing data types. Without robust governance, schema drift leads to broken pipelines, corrupted analytical queries, and silent data loss.

---

## 2. Schema Enforcement vs. Schema Evolution

Modern lakehouse architectures provide two opposing paradigms for managing incoming structural changes:

```
┌─────────────────────────────────┐     ┌─────────────────────────────────┐
│      Incoming Data Payload      │     │       Target Lakehouse Table    │
│  (Contains new/altered columns) │     │    Strict Initial Schema        │
└────────────────┬────────────────┘     └─────────────────────────────────┘
                 │                                       │
                 ▼                                       ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                           Lakehouse Engine                              │
└────────────────┬───────────────────────────────────────┬────────────────┘
                 │                                       │
                 ▼                                       ▼
┌─────────────────────────────────┐     ┌─────────────────────────────────┐
│        Schema Enforcement       │     │        Schema Evolution         │
│  • Rejects unaligned payloads   │     │  • Automatically permits safe   │
│  • Throws runtime exceptions    │     │    structural additions         │
│  • Maximum data safety & parity │     │  • Updates table metadata JSON  │
└─────────────────────────────────┘     └─────────────────────────────────┘

```

* **Schema Enforcement (Default):** Rejects any write operation whose schema does not match the target table's schema. This protects downstream BI dashboards and machine learning models from accidental pollution.
* **Schema Evolution (`mergeSchema`):** An opt-in capability that allows engineers to explicitly permit non-breaking structural modifications to be automatically merged into the table's metadata during appends or overwrites.

---

## 3. Classifying Schema Changes: Safe vs. Breaking

Not all schema modifications are equal. Lakehouse engines categorize changes based on backward and forward compatibility:

### A. Additive Changes (Safe & Fully Supported)

* **Adding New Columns:** Appending new fields to the dataset. Existing historical rows automatically return `NULL` for the new columns when queried.
* **Widening Data Types:** Automatically promoting numeric types safely (e.g., `Integer` $\rightarrow$ `Long` or `Float` $\rightarrow$ `Double`) without data truncation.

### B. Breaking Changes (Requires Explicit Migration)

* **Dropping Columns:** Removing active fields. Can break downstream queries or models depending on those attributes.
* **Renaming Columns:** Changing a column name (e.g., `cust_id` $\rightarrow$ `customer_id`). Lakehouse engines treat this as dropping an old column and adding a new one, resulting in null values for historical data unless handled via explicit column mapping.
* **Incompatible Type Conversions:** Changing data types destructively (e.g., `String` $\rightarrow$ `Integer` or `Integer` $\rightarrow$ `Boolean`), which risks parsing errors or data corruption.

---

## 4. Column Mapping & Advanced Metadata Management

To overcome the performance cost of rewriting physical Parquet files when metadata changes occur (such as re-naming columns across terabytes of data), modern Delta Lake features **Column Mapping**:

* **Name-Based Column Mapping:** Relies on unique physical IDs assigned to columns rather than their string names. This allows columns to be renamed or dropped instantly via metadata updates in the `_delta_log` without physically rewriting underlying Parquet data files.

---

## 5. Senior Interview Deep-Dive Q&A

### Q1: How does Delta Lake ensure historical queries still work after a schema evolution event?

**Answer:**
Delta Lake separates data files from metadata. Every time a schema evolves, the new transaction log version (`NNNNNN.json`) records the updated table schema. When a user runs a time-travel query against an older version (e.g., `versionAsOf 2`), Spark reads the transaction log snapshot corresponding to version 2, applying the schema that was active at that specific point in time. Historical data files are never retroactively altered by additive schema changes.

### Q2: What is the risk of enabling schema evolution globally on all streaming ingestion pipelines?

**Answer:**
Enabling `mergeSchema = true` globally on automated streaming pipelines can be dangerous. If an upstream application accidentally sends corrupted payloads with malformed column names or unexpected data types, the lakehouse will automatically accept and propagate the drift, polluting the Silver/Gold layers and breaking downstream analytics. Best practice is to enforce strict schemas on Bronze-to-Silver boundaries and handle schema evolution through explicit, governed CI/CD pipeline deployments.

---

### 🚀 Try It Out

Execute the experiment using Gradle:

```bash
./gradlew runSchemaEvolutionExperiment
```