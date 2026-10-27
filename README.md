# 🚀 Enterprise Spark Engineering & Interview Repository

A hands-on repository for investigating **Apache Spark internals, performance engineering, and production-oriented data engineering** using Scala, Java, and Spark SQL.

This project goes beyond API examples. Each experiment connects query semantics to physical execution, observable runtime behavior, benchmark evidence, and engineering trade-offs.

<p align="left">
  <img src="https://img.shields.io/badge/Apache%20Spark-3.5.1-E25A1C?logo=apachespark&logoColor=white" alt="Apache Spark 3.5.1" />
  <img src="https://img.shields.io/badge/Scala-2.12.x-DC322F?logo=scala&logoColor=white" alt="Scala 2.12" />
  <img src="https://img.shields.io/badge/Java-17-ED8B00?logo=openjdk&logoColor=white" alt="Java 17" />
  <img src="https://img.shields.io/badge/Build-Gradle-02303A?logo=gradle&logoColor=white" alt="Gradle" />
  <img src="https://img.shields.io/badge/Focus-Performance%20Engineering-2563EB" alt="Performance Engineering" />
</p>

🔗 **Repository:** [shrikantkarvade/spark-interview-master](https://github.com/shrikantkarvade/spark-interview-master)

---

## 🧭 What This Repository Demonstrates

- 🧠 Reasoning from a query requirement to Spark's physical execution plan.
- 🔍 Investigating logical plans, physical operators, exchanges, stages, tasks, and shuffles.
- ⚙️ Comparing query shapes, partitioning strategies, and execution configurations through controlled experiments.
- 📊 Using `explain`, Spark UI, task metrics, and runtime measurements to validate hypotheses.
- 🧩 Understanding how AQE, joins, aggregation, caching, memory, and serialization affect execution.
- 🏗️ Building reusable Spark setup and experiment patterns rather than isolated snippets.

### 🔬 The Engineering Workflow

**Use case → Hypothesis → Implementation → Physical plan → Spark UI and metrics → Analysis → Trade-offs → Decision**

---

## 🛠️ Technology Stack

| Technology | Version / Usage |
|---|---|
| ☕ Java | Java 17 target/runtime |
| 🔴 Scala | 2.12.x |
| ⚡ Apache Spark | 3.5.1 |
| 🐘 Build | Gradle Wrapper + Kotlin DSL |
| 🗃️ Data processing | Spark SQL, DataFrame and Dataset APIs |
| ⚙️ Configuration | Typesafe Config |
| 📝 Logging | Log4j 2 |
| ✅ Tests | JUnit 4 |

> Exact local runtime patch versions may differ. See `gradle.properties` and `build.gradle.kts` for the project's dependency configuration.

---

## 🗺️ Project Progress

The repository has progressed beyond the initial bootstrap and first query-shape benchmark. Implemented areas include foundational Spark examples and a growing set of focused experiments around query planning, data-source pushdown, partitioning, joins, aggregation, and resource behavior.

### 🧱 Phase 1 — Foundation and Execution Fundamentals

| Area | Coverage |
|---|---|
| 🏁 Repository bootstrap | Gradle Kotlin DSL, Scala/Spark dependencies, project layout |
| 🧑‍💻 Spark entry points | Scala, Java, and Spark SQL foundation examples |
| 🏢 Enterprise session setup | Shared SparkSession creation and environment-aware configuration patterns |
| 🧭 Query planning | Logical-to-physical plan inspection |
| 📥 Data-source optimization | Filter and projection pushdown experiments |
| 🧮 Partitioning | Baseline, repartition, and coalesce experiments |
| ⏱️ Performance baseline | Repeatable synthetic workloads, explicit parameters, plan inspection, Spark UI support |

### ⚙️ Phase 2 — Query Execution and Performance Experiments

Experiments are designed to isolate a particular behavior rather than change several settings at once.

- 🔝 **Query shape and Top-N:** compare aggregation-only, aggregation with full ordering, and aggregation with `ORDER BY ... LIMIT`.
- 🧾 **Physical plans:** inspect operators such as `HashAggregate`, `Exchange`, `Sort`, and `TakeOrderedAndProject`.
- 🔀 **Shuffle behavior:** explore shuffle partition counts and the effect of AQE where enabled.
- 🤝 **Joins:** investigate join strategy selection and the implications of broadcast settings, shuffle, and AQE.
- 🧮 **Partitioning:** compare existing partition layouts with repartitioning and coalescing.
- 🧠 **Aggregation resources:** examine how workload shape and configuration affect execution and spill behavior.
- 💾 **Memory experiments:** compare selected Spark memory settings in separate application runs when the setting is fixed at SparkContext construction time.

> 🚧 Not every topic above is a completed production solution; this is an experimental repository. Check the source and associated notes before treating a topic as complete.

### 📊 Performance Case Study — Full Ordering vs. Top-N

A foundational benchmark uses a deterministic synthetic workload with configurable row count, customer count, and shuffle partition count. It compares query shapes such as:

1. `GROUP BY` plus `ORDER BY`
2. `GROUP BY` only
3. `GROUP BY` plus `ORDER BY ... LIMIT 100`

**💡 Key lesson:** query semantics influence the physical plan. When only the highest N records are required, Spark may use a Top-N operator such as `TakeOrderedAndProject` rather than performing the same global range-partitioning and sorting work required by a full ordered result. The aggregation shuffle may still be necessary to compute correct grouped results.

⏱️ Benchmark timings depend on hardware, JVM state, Spark configuration, and workload parameters. Treat execution plans, shuffle volume, task distribution, spill, GC, and AQE behavior as evidence alongside elapsed time—not as universal performance guarantees.

---

## 🧑‍🔬 How to Explore the Code

Start with these repository areas:

- 📂 [`src/main/scala`](src/main/scala) — Scala implementations and experiments.
- 📂 [`src/main/java`](src/main/java) — Java implementations.
- 📂 [`src/main/resources`](src/main/resources) — application and logging configuration.
- 📂 [`conf`](conf) — project configuration files.
- 🏗️ [`build.gradle.kts`](build.gradle.kts) — Gradle tasks, dependencies, and JVM options.
- ⚙️ [`gradle.properties`](gradle.properties) — version and Gradle properties.
- 📚 [`docs`](docs) — documentation and experiment findings, where available.

> 📌 The repository layout may evolve as modules are added. Prefer the actual source tree over assuming that every planned directory or document already exists.

---

## 🚀 Getting Started

### 📋 Prerequisites

- Git
- JDK 17
- Windows, Linux, or macOS with a supported shell
- Internet access for Gradle to resolve dependencies on the first build

Check Java:

```bash
java -version
```

### 📥 Clone the Repository

```bash
git clone https://github.com/shrikantkarvade/spark-interview-master.git
cd spark-interview-master
```

### 🏗️ Build the Project

**Linux / macOS / Git Bash**

```bash
./gradlew build
```

**Windows Command Prompt / PowerShell**

```powershell
.\gradlew.bat build
```

### 🧰 Discover Available Experiment Tasks

Gradle task names change as the project grows. List the tasks defined in the current checkout:

```bash
./gradlew tasks --all
```

Run an experiment using the exact task name shown by Gradle. The repository has foundation tasks for Scala, Java, SQL, performance benchmarking, session setup, query planning, pushdown, and partitioning. Consult `build.gradle.kts` for the current task-to-class mapping.

A previously configured foundation benchmark can be launched with:

```bash
./gradlew runSparkFoundationBenchmark
```

To pass benchmark arguments, use the syntax supported by that task in the current checkout. For example, the foundation benchmark has used row count, customer count, and shuffle partition count as positional parameters:

```bash
./gradlew runSparkFoundationBenchmark --args="10000000 1000000 64"
```

If an experiment supports a Spark UI pause option, consult that experiment's argument parsing or comments for the exact flag syntax.

### 🖥️ Open the Spark UI

For local Spark applications, the UI commonly starts at:

```text
http://localhost:4040
```

The port can differ if 4040 is already occupied. Keep the application running while inspecting the UI; a completed application may no longer expose its live UI unless event logging/history-server support has been configured.

---

## 📈 Benchmarking Principles

The repository emphasizes explainable measurements:

1. 🎯 **State the question and hypothesis** before running the experiment.
2. 🧪 **Control the variables:** keep input data and parameters consistent when comparing scenarios.
3. ⏳ **Understand the timing:** determine whether data generation, caching, and materialization are included.
4. 🧾 **Inspect the plans:** examine logical and physical execution.
5. 🖥️ **Inspect the Spark UI:** review relevant stage and task metrics.
6. 📊 **Compare runtime signals:** shuffle read/write, partition distribution, spill, GC, and task duration where available.
7. 🧠 **Explain the result:** document why observed behavior matches—or differs from—the hypothesis.
8. 📝 **Record limitations and trade-offs** alongside the conclusion.

> ⚠️ Do not compare elapsed times blindly. Local runs are useful for studying execution mechanics, but they do not directly predict a distributed production cluster's performance.

---

## 🧠 Engineering Questions Explored

### 🧭 Query Planning and Execution
- How does Spark move from a logical plan to a physical plan?
- Which operators introduce exchanges or sorts?
- How do jobs, stages, and tasks relate to the query?
- How can the same business requirement be expressed with different execution costs?

### 🔀 Partitioning and Shuffle
- What changes when input or shuffle partition counts change?
- When is `repartition` worth its shuffle cost?
- When can `coalesce` reduce partitions without a full shuffle?
- Does AQE coalesce shuffle partitions or change the final plan?

### 🤝 Joins and Aggregation
- What determines the join strategy?
- When is broadcasting useful, and what are its memory trade-offs?
- How do partial and final aggregation reduce or redistribute data?
- How can skew and uneven task durations be identified?

### 💾 Memory and Runtime Behavior
- When does Spark spill?
- How should execution time be interpreted alongside GC and task metrics?
- Which settings are fixed when the SparkContext is created?
- Why should memory experiments isolate variables and often use separate application runs?

### 🏭 Production-Minded Engineering
- How should SparkSession creation and configuration be centralized?
- How should experiments accept and validate command-line arguments?
- How can a benchmark remain repeatable and understandable?
- What evidence is needed before accepting an optimization?

---

## 🛣️ Roadmap

The following are ongoing directions, not a claim that every item is already implemented.

- 🔀 Deepen shuffle, join, and aggregation experiments.
- ⚖️ Expand data-skew and AQE investigations.
- 🧠 Continue memory, spill, serialization, and garbage-collection experiments.
- 📚 Improve experiment documentation with physical-plan excerpts and Spark UI evidence.
- 🌊 Extend Structured Streaming topics such as checkpoints, watermarks, state stores, recovery, and delivery semantics.
- 🏞️ Develop larger end-to-end data engineering and lakehouse case studies.
- ☁️ Add cloud-oriented implementations and realistic datasets in a separate, clearly scoped project where appropriate.

Progress is incremental: completed work should be supported by source code, reproducible commands, and documented evidence.

---

## 🎯 Interview Preparation

This repository can help prepare for senior and lead data engineering interviews by practicing explanations that connect code to distributed execution:

> ❓ Why did Spark choose this physical operator? What data moved across the shuffle? What did the Spark UI show? Which alternative would you test, and what trade-offs would it introduce?

Topics include Spark SQL, Catalyst planning, execution stages, joins, aggregation, partitioning, AQE, memory behavior, Structured Streaming, and performance diagnosis.

---

## 👨‍💻 Author

**Shrikant Karvade**

Big Data and Data Engineering professional working with Java, Scala, Apache Spark, Hadoop, distributed data processing, and performance engineering.

- 🐙 GitHub: [@shrikantkarvade](https://github.com/shrikantkarvade)
- 🚀 Project: [Enterprise Spark Engineering & Interview Repository](https://github.com/shrikantkarvade/spark-interview-master)

---

## 🤝 Contributing and Feedback

Suggestions, issue reports, and discussions are welcome. Please include the Spark version, experiment parameters, relevant plan output, and Spark UI/task metrics when reporting a performance observation.

## 📄 License

This project is distributed under the [MIT License](LICENSE).
