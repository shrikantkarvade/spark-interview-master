# Module 5.5: Medallion Architecture Multi-Hop Pipeline (Bronze $\rightarrow$ Silver $\rightarrow$ Gold)

## 1. Introduction to the Medallion Architecture

Enterprise data platforms often suffer from data swamp syndrome—a chaotic accumulation of unorganized files in cloud object storage that lacks structure, quality guarantees, or clear lineage.

The **Medallion Architecture** is a data design pattern that organizes data into progressive quality tiers as it flows through the platform. By breaking ingestion into distinct, logical hops, data engineering teams can ensure incremental processing, flawless auditability, and reliable consumption for business intelligence and machine learning.

```
┌─────────────────────────────────┐
│          Source Systems         │
│  (Kafka, Postgres CDC, APIs)    │
└────────────────┬────────────────┘
                 │
                 ▼
┌─────────────────────────────────┐
│         BRONZE LAYER            │
│  • Raw, immutable ingestion     │
│  • Append-only storage          │
│  • Full historical fidelity     │
└────────────────┬────────────────┘
                 │
                 ▼
┌─────────────────────────────────┐
│         SILVER LAYER            │
│  • Cleaned, deduplicated data   │
│  • Validated via DLQ / Quality  │
│  • Conformed schema & enforcements│
└────────────────┬────────────────┘
                 │
                 ▼
┌─────────────────────────────────┐
│          GOLD LAYER             │
│  • Business-level aggregations  │
│  • Dimensional models / KPIs    │
│  • Ready for BI & ML consumers │
└─────────────────────────────────┘

```

---

## 2. Deep Dive: The Three Quality Tiers

### A. Bronze Layer (Raw Zone)

* **Objective:** Land raw data from upstream sources (Kafka streams, batch files, database CDC logs) as quickly and faithfully as possible with zero transformation.
* **Characteristics:**
* Append-only ingestion mode.
* Preserves exact source schemas and payload structures (JSON, CSV, Parquet).
* Serves as the immutable source of truth and disaster recovery backup for the entire platform.



### B. Silver Layer (Conformed Zone)

* **Objective:** Transform raw, noisy Bronze data into clean, structured, and validated datasets ready for enterprise consumption.
* **Characteristics:**
* Deduplication of records based on primary business keys.
* Integration of Change Data Capture (CDC) upserts (`MERGE INTO`).
* Enforcement of data quality rules (filtering out nulls or invalid ranges and routing failures to a Dead Letter Queue / DLQ).
* Standardization of column naming conventions and data types.



### C. Gold Layer (Curated / Analytical Zone)

* **Objective:** Aggregate and structure data into business-level domain models, star schemas, and executive metrics.
* **Characteristics:**
* High-level business summaries (e.g., monthly departmental payroll, active customer metrics, cohort retention).
* Highly optimized for fast BI dashboard queries (Tableau, PowerBI) and feature stores for machine learning models.



---

## 3. Pipeline Orchestration & State Management

Managing multi-hop pipelines requires careful coordination between batch and streaming workloads:

* **Idempotency:** Each hop must be fully idempotent so that pipeline retries do not duplicate records or corrupt downstream aggregations.
* **Incremental Processing:** Instead of reprocessing historical tables, Silver and Gold hops read only newly arrived deltas from the previous layer using Delta time travel or Change Data Feed (CDF).

---

## 4. Senior Interview Deep-Dive Q&A

### Q1: Why separate the Silver layer from the Gold layer? Can't BI tools query Silver directly?

**Answer:**
While BI tools *can* query Silver tables, doing so for enterprise reporting is inefficient and risky. Silver tables contain granular, row-level operational records that require expensive joins and filters at query time, leading to slow dashboard performance. The Gold layer pre-aggregates and structures data into dimensional models (fact and dimension tables), shielding business analysts from underlying data complexities and ensuring consistent metric definitions across the enterprise.

### Q2: How do you handle breaking schema changes in upstream source systems across a Medallion pipeline?

**Answer:**
Upstream schema changes are isolated at the **Bronze boundary**. Because Bronze is append-only and schema-flexible, raw payloads with new columns land without failing. However, to prevent polluting the Silver and Gold layers, ingestion jobs must explicitly validate incoming schemas against conformed models. Additive changes are promoted to Silver via governed schema evolution (`mergeSchema`), while breaking changes (like renamed or deleted primary columns) trigger alerts and route malformed payloads to a quarantine DLQ for engineering review before downstream promotion.

---

### 🚀 Try It Out

Execute the experiment using Gradle:

```bash
./gradlew runMedallionExperiment
```