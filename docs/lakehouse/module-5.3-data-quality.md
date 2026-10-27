# Module 5.3: Data Quality, Governance & Expectation Engines

---

## 1. The Data Quality Imperative in Enterprise Lakehouses

In modern data engineering, the transition from rigid relational databases to open-schema object storage introduces a major operational hazard: **"Garbage In, Garbage Out."** Without strict upstream governance, downstream BI dashboards, machine learning models, and executive reporting are quickly compromised by silent data corruption, null injection, schema drift, or out-of-range metrics.

Enterprise-grade lakehouses must treat data quality not as an afterthought, but as an automated, continuous, in-flight validation process.

---

## 2. Declarative Data Quality & Expectation Frameworks

Rather than writing custom, imperative validation scripts for every pipeline, modern architectures rely on **declarative expectation engines** (such as Apache Deequ, Great Expectations, or native Delta Live Tables expectations).

```
┌─────────────────────────────────┐     ┌─────────────────────────────────┐
│     Raw Incoming Data Stream    │────►│      Expectation Engine         │
│  (JSON, Parquet, Kafka Batches) │     │  (Completeness, Uniqueness,     │
└─────────────────────────────────┘     │   Validity, Range Assertions)   │
                                        └──────────────┬──────────────────┘
                                                       │
                                        ┌──────────────┴──────────────┐
                                        ▼                             ▼
                        ┌───────────────────────────────┐ ┌───────────────────────────────┐
                        │         Valid Records         │ │        Invalid Records        │
                        │   (Committed to Silver Layer) │ │   (Isolated to DLQ / Drop)    │
                        └───────────────────────────────┘ └───────────────────────────────┘

```

### Core Expectation Categories:

* **Completeness:** Ensures critical columns (e.g., `transaction_id`, `customer_id`, `timestamp`) do not contain null values beyond an acceptable threshold.
* **Uniqueness:** Validates primary keys or business identifiers to prevent duplicate record injection.
* **Validity / Range:** Enforces domain constraints (e.g., `age >= 0`, `amount > 0.0`, `status IN ('ACTIVE', 'PENDING', 'CLOSED')`).
* **Consistency:** Cross-table or cross-column validations (e.g., `shipping_date >= order_date`).

---

## 3. Error Handling Strategies: Circuit Breakers vs. Quarantine

When data quality constraints fail, data engineers face three primary architectural patterns:

| Strategy | Behavior | When to Use |
| --- | --- | --- |
| **Fail the Pipeline (Circuit Breaker)** | Halts execution immediately upon encountering data quality violations. | Mission-critical financial or compliance pipelines where partial data ingestion is legally or operationally unacceptable. |
| **Drop & Alert** | Silently discards invalid records and logs an metric alert. | Low-value telemetry, clickstream analytics, or non-critical log processing where data loss is tolerable. |
| **Quarantine Pattern (DLQ)** | Splits the stream into valid records (which proceed downstream) and malformed records (which are routed to a Dead Letter Queue table for inspection). | Enterprise-grade multi-hop pipelines (Bronze $\rightarrow$ Silver) requiring zero data loss and root-cause analysis. |

---

## 4. The Quarantine Pattern & Dead Letter Queue (DLQ) Architecture

The Quarantine Pattern ensures high pipeline resilience by isolating bad records without stopping ingestion.

### Implementation Workflow:

1. **Validation Rule Evaluation:** Spark evaluates incoming rows against a suite of rules using conditional expressions or data quality libraries.
2. **Data Splitting:**
* **Pass:** Clean records are tagged and written directly to the Silver analytical layer.
* **Fail:** Malformed records are augmented with metadata columns (`_error_reason`, `_failed_rule`, `_ingest_timestamp`) and written to a dedicated DLQ table.


3. **Remediation Loop:** Dataops teams query the DLQ table, fix upstream parser bugs or data source anomalies, and replay corrected records back into the pipeline.

---

## 5. Data Governance & Lineage Metadata

Quality enforcement cannot exist without visibility. Modern lakehouses integrate data governance platforms (such as Apache Atlas, Unity Catalog, or OpenLineage) to track:

* **Column-Level Lineage:** Exactly which transformation steps, joins, and filters produced a given metric in a Gold tier table.
* **Data Contracts:** Formal agreements between upstream producers and downstream consumers defining schema definitions, SLA expectations, and expected quality thresholds.

---

## 6. Senior Interview Deep-Dive Q&A

### Q1: When should you use a pipeline circuit breaker versus the Quarantine pattern?

**Answer:**
The choice depends on the business impact of data errors.

* Use a **Circuit Breaker** when dealing with core financial ledgers, inventory balances, or regulatory reporting where ingesting corrupt data could lead to severe financial discrepancies or legal penalties. Halting the pipeline forces immediate intervention.
* Use the **Quarantine Pattern (DLQ)** for high-throughput operational systems, IoT telemetry, or customer events where individual record corruption shouldn't block the ingestion of thousands of valid records. It maintains pipeline uptime while preserving bad payloads for forensic analysis.

### Q2: How do you scale data quality checks in large distributed Spark pipelines without creating memory bottlenecks?

**Answer:**
Running expensive metric calculations or uniqueness checks across multi-terabyte datasets can cause driver memory exhaustion or massive shuffles. To scale efficiently:

1. **Incremental Validation:** Apply expectations only to newly arrived micro-batches or partitions rather than rescanning historical tables.
2. **Pushdown Constraints:** Leverage data lake file statistics (min/max values stored in transaction logs) to evaluate range constraints without reading full row data into memory.
3. **Distributed Assertions:** Use optimized libraries like Deequ that compute quality metrics using single-pass sketching and aggregation algorithms over distributed RDD partitions.

---

### 🚀 Try It Out

Execute the experiment using Gradle:

```bash
./gradlew runCdcExperiment
```