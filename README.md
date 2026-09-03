# 🚀 Spark Interview Master

A hands-on, engineering-focused repository for mastering **Apache Spark, Big Data Engineering, distributed systems, and production-grade data processing** through real-world problems, experiments, performance analysis, and interview preparation.

This repository is designed to demonstrate **senior/lead-level engineering expertise**, not just knowledge of Spark APIs.

---

# 🎯 What This Repository Demonstrates

The goal is to answer questions such as:

* Why is a Spark job slow?
* Where exactly is time being spent?
* What causes a shuffle?
* How does Spark choose a physical execution strategy?
* Why does one query require multiple stages?
* How does Adaptive Query Execution change execution?
* Why can `ORDER BY ... LIMIT 100` be dramatically cheaper than a full `ORDER BY`?
* How do partitioning, joins, aggregations, serialization, and memory affect performance?
* How do we diagnose Spark problems using the Spark UI and execution plans?
* What design choices make a Spark application production-ready?

The repository combines:

**Use Case → Problem → Investigation → Spark Internals → Solution → Benchmark → Trade-offs**

---

# 🧠 Engineering-First Approach

This is not a collection of interview questions or copied Spark examples.

Each module focuses on understanding **why Spark behaves the way it does**.

The approach is:

```text
Real-world Use Case
        ↓
Problem Definition
        ↓
Implementation
        ↓
Execution Plan
        ↓
Spark UI
        ↓
Performance Metrics
        ↓
Root Cause
        ↓
Optimization
        ↓
Trade-offs
        ↓
Engineering Decision
```

The objective is to develop the ability to reason about distributed systems rather than simply memorize Spark concepts.

---

# 🔍 Example Engineering Questions

Examples of the types of questions investigated throughout the repository:

### Spark Execution

* What happens between a DataFrame transformation and actual execution?
* How does Spark build a logical plan?
* How does Catalyst optimize the plan?
* How does Spark generate the physical plan?
* What is WholeStageCodegen?
* Why does Spark create multiple stages?

### Shuffle

* Why does a `groupBy` cause a shuffle?
* Why can a shuffle be expensive?
* How much data is actually shuffled?
* How does Spark partition shuffle data?
* How does AQE coalesce shuffle partitions?

### Aggregation

* What is partial aggregation?
* Why does Spark perform aggregation before the shuffle?
* How does `HashAggregate` work?
* What happens when aggregation state becomes large?

### Ordering and Top-N

* Why does a global `ORDER BY` require additional coordination?
* Why can `ORDER BY ... LIMIT 100` avoid a full global sort?
* What is `TakeOrderedAndProject`?
* When does Spark use range partitioning?
* Why can Top-N queries be significantly cheaper than full ordering?

### Performance Engineering

* Is the problem CPU, memory, I/O, shuffle, skew, or serialization?
* Are partitions balanced?
* Are tasks becoming stragglers?
* Is Spark spilling to disk?
* Is garbage collection affecting execution?
* Are we measuring the actual workload or only part of the execution?

---

# 🛠️ Technology Stack

| Technology   | Version           |
| ------------ | ----------------- |
| Java         | 17.0.20.101       |
| Scala        | 2.12.18           |
| Apache Spark | 3.5.1             |
| Gradle       | 9.6.0             |
| Build Tool   | Gradle Kotlin DSL |
| Build System | Gradle Wrapper    |
| IDE          | IntelliJ IDEA     |

---

# 🏗️ Implementation Strategy

The repository primarily uses:

* Scala
* Java
* Apache Spark
* Spark SQL
* DataFrame API
* Dataset API

Implementations focus on:

* readable production-style code
* deterministic test data
* measurable benchmarks
* execution-plan analysis
* Spark UI investigation
* performance trade-offs
* engineering documentation

---

# 📂 Project Structure

```text
spark-interview-master/
│
├── datasets/
│   ├── batch/
│   ├── streaming/
│   └── generated/
│
├── docs/
│   ├── architecture/
│   ├── case-studies/
│   ├── performance/
│   │   └── module-1.2-query-shape-top-n.md
│   └── interview/
│
├── src/
│   ├── main/
│   │   ├── java/
│   │   │   └── com/shrikant/sparkjava/
│   │   │
│   │   ├── scala/
│   │   │   └── com/shrikant/spark/
│   │   │
│   │   └── resources/
│   │       ├── application.conf
│   │       └── log4j2.properties
│   │
│   └── test/
│       ├── java/
│       └── scala/
│
├── build.gradle.kts
├── settings.gradle.kts
├── gradle.properties
├── gradlew
├── gradlew.bat
├── LICENSE
└── README.md
```

---

# 📚 Project Modules

## Phase 1 — Spark Foundation

### Module 1.1 — Repository Bootstrap ✅

Established the engineering foundation of the repository.

Focus areas:

* Gradle Kotlin DSL
* Scala integration
* Java integration
* Spark dependencies
* Project structure
* Testing foundation
* Git/GitHub workflow
* Build reproducibility

---

### Module 1.2 — Spark Foundation: Query Execution & Performance ✅

This module moves beyond basic Spark API usage into **execution-engine reasoning**.

Focus areas:

* SparkSession
* DataFrame
* Spark SQL
* Transformations
* Actions
* Lazy evaluation
* Logical plans
* Physical plans
* `explain(true)`
* Jobs
* Stages
* Tasks
* Shuffle
* Partitioning
* Partial aggregation
* Final aggregation
* Global ordering
* Top-N execution
* Adaptive Query Execution
* Spark UI
* Task-level metrics
* Benchmark methodology

### Engineering Case Study

**Why can `ORDER BY ... LIMIT 100` be dramatically cheaper than `ORDER BY` in Spark?**

A deterministic benchmark was created using:

* 10,000,000 rows
* 1,000,000 customers
* 6 initial input partitions
* 64 configured shuffle partitions
* AQE enabled
* Cached benchmark dataset
* `local[*]` execution

Three query shapes were compared:

```text
Scenario A
GROUP BY + ORDER BY

Scenario B
GROUP BY ONLY

Scenario C
GROUP BY + ORDER BY + LIMIT 100
```

### Key Execution Difference

Full ordering produces an execution path conceptually similar to:

```text
HashAggregate
      ↓
Exchange
(hashpartitioning)
      ↓
HashAggregate
      ↓
Exchange
(rangepartitioning)
      ↓
Sort
      ↓
Ordered result
```

The Top-N query instead uses:

```text
HashAggregate
      ↓
Exchange
(hashpartitioning)
      ↓
HashAggregate
      ↓
TakeOrderedAndProject(100)
      ↓
Top 100 result
```

The important difference is that the Top-N query does **not require the same global range-partitioning exchange and separate global sort** used by the full ordering query.

The first aggregation shuffle remains necessary because Spark must bring records belonging to the same customer together before calculating the final customer-level aggregate.

### Spark UI Evidence

The aggregation stage for the Top-N workload showed:

* 6 tasks
* approximately 0.9–1.0 seconds per task
* approximately 5 ms GC per task
* approximately 33.3–33.4 MiB input per task
* approximately 10.7 MiB shuffle output per task
* approximately 1,000,000 shuffle records per task
* process-local execution
* no failed tasks
* no meaningful task imbalance
* no spill
* no significant GC pressure

The evidence indicates that the aggregation workload was well balanced and that there was no meaningful skew or resource bottleneck in this stage.

### Engineering Lesson

> **Query semantics influence Spark's physical execution strategy.**

A shuffle is not inherently bad.

The important engineering questions are:

1. Why is the shuffle required?
2. What business requirement causes it?
3. How much data is being shuffled?
4. Is the shuffle balanced?
5. Can the query semantics allow Spark to use a cheaper physical operator?
6. Are we paying for work that the business requirement does not actually need?

Detailed analysis:

`docs/performance/module-1.2-query-shape-top-n.md`

---

### Module 1.3 — Enterprise SparkSession ⏳

Planned focus:

* SparkSession lifecycle
* application configuration
* environment-specific configuration
* local vs cluster execution
* session isolation
* production considerations

---

### Module 1.4 — Configuration ⏳

Planned focus:

* Spark configuration
* executor configuration
* driver configuration
* shuffle configuration
* memory configuration
* serialization
* adaptive execution
* environment-specific configuration

---

### Module 1.5 — Logging ⏳

Planned focus:

* structured logging
* log levels
* application diagnostics
* production troubleshooting
* log4j configuration

---

### Module 1.6 — Dataset Repository ⏳

Planned focus:

* reusable datasets
* deterministic data generation
* test fixtures
* batch datasets
* streaming datasets
* benchmark datasets

---

### Module 1.7 — Coding Standards ⏳

Planned focus:

* Scala coding standards
* Java coding standards
* Spark coding patterns
* API selection
* maintainability
* production-quality implementation

---

# ⚡ Spark Performance Engineering

Performance engineering is a major component of this repository.

Topics include:

* Partition sizing
* Shuffle optimization
* Broadcast joins
* Sort-merge joins
* Hash joins
* Join strategy selection
* Data skew
* AQE
* Predicate pushdown
* Projection pruning
* Column pruning
* Caching
* Persistence
* Serialization
* Memory management
* Spill behavior
* Garbage collection
* Task parallelism
* File sizing
* Small-file problems
* Parquet optimization
* Bucketing
* Partition pruning
* Query plan analysis

The goal is not to memorize optimization techniques.

The goal is to understand:

> **Why the optimization works, when it works, and when it can make things worse.**

---

# 🚀 Planned Advanced Topics

Future modules will investigate topics such as:

### Spark Execution Engine

* Catalyst Optimizer
* Tungsten
* WholeStageCodegen
* Adaptive Query Execution
* Query stages
* Exchange operators

### Joins

* Broadcast Hash Join
* Sort Merge Join
* Shuffled Hash Join
* Broadcast thresholds
* Join hints
* Skew joins
* Large-large joins

### Data Skew

* Identifying skew
* Salting
* AQE skew handling
* Hot keys
* Partition imbalance
* Straggler analysis

### Memory

* Executor memory
* Execution memory
* Storage memory
* Unified memory manager
* Spill
* GC
* Off-heap memory

### Storage

* Parquet
* ORC
* Compression
* Predicate pushdown
* Column pruning
* Partition pruning
* Small-file optimization

### Streaming

* Structured Streaming
* Checkpointing
* Watermarks
* State management
* Exactly-once semantics
* Kafka integration
* Late-arriving data

---

# 🏢 Real-World Data Engineering Scenarios

The repository will model problems commonly encountered in enterprise data platforms.

Examples:

### Financial Risk

* Market Risk
* RWA calculations
* FRTB
* Regulatory reporting
* Large-scale aggregation
* Cross-region processing

### Data Pipelines

* Batch processing
* Incremental processing
* Historical reprocessing
* Data quality
* Schema evolution

### Streaming

* Kafka ingestion
* Real-time enrichment
* Stateful processing
* Event-time processing

### Enterprise Platforms

* Large datasets
* Multi-stage pipelines
* SLA-driven workloads
* Failure recovery
* Observability
* Performance optimization

---

# 📊 Performance Case Studies

| Case Study          | Root Cause                                                        | Engineering Solution                                             |
| ------------------- | ----------------------------------------------------------------- | ---------------------------------------------------------------- |
| `ORDER BY` vs Top-N | Full global ordering requires additional coordination and sorting | Use Top-N semantics when only the highest N records are required |
| Large Join          | Excessive shuffle                                                 | Evaluate join strategy and partitioning                          |
| Data Skew           | Uneven partition sizes                                            | AQE/skew handling/salting                                        |
| Small Files         | Excessive task and metadata overhead                              | File compaction and sizing                                       |
| Excessive GC        | Memory pressure / object overhead                                 | Reduce allocations and tune memory usage                         |
| Slow Aggregation    | Large shuffle and aggregation state                               | Partial aggregation, partitioning, AQE                           |

---

# 🧪 Testing Strategy

Testing will cover multiple levels.

### Unit Tests

Validate:

* transformations
* business logic
* utility functions
* edge cases

### Spark Tests

Validate:

* DataFrame transformations
* aggregations
* joins
* partitioning behavior
* schema correctness

### Integration Tests

Validate:

* end-to-end pipelines
* input/output behavior
* external integrations

### Performance Tests

Measure:

* execution time
* shuffle volume
* task distribution
* spill
* GC
* partition behavior
* execution plans

---

# 🎯 Interview Preparation

The repository is also structured to support senior-level Spark interviews.

Instead of simply answering:

> What is a shuffle?

The objective is to answer:

> Why did this particular query generate a shuffle, how much data did it move, how did the physical plan implement it, what did the Spark UI show, and what alternatives could reduce the cost?

Topics include:

* Spark architecture
* Spark execution
* RDD vs DataFrame vs Dataset
* Catalyst
* Tungsten
* AQE
* Joins
* Shuffle
* Partitioning
* Data skew
* Memory management
* Structured Streaming
* Kafka
* Performance tuning
* Debugging
* System design

---

# 🧩 Engineering Case Studies

Each major case study follows:

```text
Business Requirement
        ↓
Technical Problem
        ↓
Initial Implementation
        ↓
Physical Plan
        ↓
Spark UI Investigation
        ↓
Performance Analysis
        ↓
Optimization
        ↓
Benchmark
        ↓
Trade-offs
        ↓
Final Engineering Decision
```

This structure is intentionally designed to demonstrate **engineering judgment**, rather than API familiarity alone.

---

# 🌍 Build in Public

This repository is being developed publicly as an ongoing engineering journey.

Each major module can include:

* implementation
* benchmark
* execution-plan analysis
* Spark UI evidence
* technical documentation
* engineering case study
* LinkedIn write-up

The objective is to demonstrate practical expertise in:

* Apache Spark
* Big Data Engineering
* distributed systems
* performance engineering
* production architecture
* technical problem solving

---

# 🗺️ Roadmap

## Phase 1 — Foundation

* [x] 1.1 Repository Bootstrap
* [x] 1.2 Spark Foundation: Query Execution & Performance
* [ ] 1.3 Enterprise SparkSession
* [ ] 1.4 Configuration
* [ ] 1.5 Logging
* [ ] 1.6 Dataset Repository
* [ ] 1.7 Coding Standards

## Phase 2 — Spark Internals

* [ ] Catalyst Optimizer
* [ ] Tungsten
* [ ] WholeStageCodegen
* [ ] Adaptive Query Execution
* [ ] Query Stages
* [ ] Shuffle Internals

## Phase 3 — Performance Engineering

* [ ] Join Optimization
* [ ] Data Skew
* [ ] Partitioning
* [ ] Memory Management
* [ ] Serialization
* [ ] Spill Analysis
* [ ] GC Analysis

## Phase 4 — Production Spark

* [ ] Structured Streaming
* [ ] Kafka
* [ ] Checkpointing
* [ ] Monitoring
* [ ] Failure Recovery
* [ ] Production Architecture

## Phase 5 — Advanced Data Engineering

* [ ] Large-scale Data Pipelines
* [ ] Incremental Processing
* [ ] Data Quality
* [ ] Schema Evolution
* [ ] Lakehouse Architecture
* [ ] End-to-End Enterprise Case Studies

---

# ▶️ Running the Project

Build the project:

```bash
./gradlew build
```

Run Spark Foundation benchmark:

```bash
./gradlew runSparkFoundationBenchmark
```

Run the Module 1.2 benchmark with explicit parameters:

```bash
./gradlew runSparkFoundationBenchmark --args="10000000 1000000 64 --ui-pause"
```

Parameters:

```text
10000000  → number of rows
1000000   → number of customers
64        → configured shuffle partitions
--ui-pause → pause after each scenario for Spark UI inspection
```

Spark UI:

```text
http://localhost:4040
```

---

# 🔬 Benchmarking Philosophy

Benchmarks in this repository are designed to be reproducible and explainable.

The methodology aims to:

* use deterministic datasets
* materialize/cache data before comparison where appropriate
* consume query results
* inspect execution plans
* inspect Spark UI
* examine task-level metrics
* compare physical execution strategies
* distinguish wall-clock time from task-level metrics
* avoid drawing conclusions from runtime alone

Absolute timings are machine-dependent, particularly for `local[*]` benchmarks.

The more important evidence is:

* physical plan shape
* number of exchanges
* shuffle volume
* partition distribution
* task behavior
* spill
* GC
* AQE behavior
* operator selection

---

# 💡 Engineering Philosophy

> **Measure first. Optimize second.**

A Spark optimization should never be accepted simply because it sounds faster.

A good optimization should answer:

1. What was slow?
2. Why was it slow?
3. What changed?
4. Why should the change help?
5. Did the physical plan change?
6. Did the Spark UI confirm the expected behavior?
7. Did the benchmark improve?
8. What trade-offs were introduced?

---

# 👨‍💻 Author

**Shrikant Karvade**

Senior Big Data / Data Engineering professional with extensive experience in:

* Apache Spark
* Scala
* Java
* Hadoop
* Hive
* Kafka
* Big Data architecture
* Data pipelines
* Performance engineering
* Enterprise data platforms

---

# ⭐ Why This Repository Exists

The purpose of this repository is to build a practical reference for **senior-level Spark and Big Data engineering**.

It is intentionally focused on:

**Understanding → Experimentation → Measurement → Optimization → Engineering Judgment**

rather than simply collecting interview questions and answers.

The ultimate goal is to demonstrate the ability to take a real distributed-data problem, understand what the engine is doing internally, measure its behavior, identify the bottleneck, and make an informed engineering decision.