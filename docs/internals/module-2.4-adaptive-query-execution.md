```markdown
# Module 2.4 — Adaptive Query Execution (AQE) Runtime Re-Optimization

## Overview

Traditional Cost-Based Optimizers (CBO) make optimization decisions—such as join strategy selection, partitioning counts, and broadcast thresholds—*before* query execution begins. These static decisions rely on catalog statistics, which are often stale, inaccurate, or completely absent (especially after intermediate transformations and shuffles).

**Adaptive Query Execution (AQE)**, introduced natively in Spark 3.0, re-optimizes the physical plan **mid-query** at stage boundaries as actual runtime statistics become available from completed shuffle stages.

This experiment investigates AQE from an execution-engineering perspective, examining how Spark dynamically adapts execution plans at runtime.

The objective is to understand:
- Why static compile-time optimization fails in complex DAGs.
- How AQE intercepts query execution between shuffle stages.
- The three core pillars of AQE: Dynamic Partition Coalescing, Dynamic Join Strategy Conversion, and Dynamic Skew Join Optimization.
- How execution plans evolve from initial physical plans to adaptive execution plans.

---

1. # Experiment Objective

The experiment creates a multi-shuffle join workload where intermediate sizes cannot be accurately predicted statically, allowing AQE to inspect runtime shuffle statistics and optimize execution on the fly.

The conceptual execution flow is:

```text
Initial Physical Plan (Compile Time)
      |
      v
Execute Shuffle Stage 1 & Stage 2
      |
      v
[AQE Interception & Statistics Collection]
      |
      +---> 1. Coalesce small shuffle partitions (Dynamic Partition Coalescing)
      +---> 2. Convert Sort-Merge Join to Broadcast Hash Join if shuffle size < threshold
      +---> 3. Split skewed partitions into smaller sub-tasks (Dynamic Skew Join)
      |
      v
Optimized Adaptive Physical Plan Execution

```

---

# 2. Benchmark Configuration

| Parameter | Value |
| --- | --- |
| Spark Version | 3.5.1 |
| AQE Enabled | `true` |
| Partition Coalescing | `true` |
| Skew Join Enabled | `true` |
| Execution Mode | Local |
| Spark UI | `http://localhost:4040` |

---

# 3. Core Pillars of AQE

## 3.1 Dynamic Partition Coalescing

* **The Problem:** Developers often configure a high number of shuffle partitions (e.g., `spark.sql.shuffle.partitions = 200`) to prevent OOM on large datasets. However, for smaller intermediate stages, this creates hundreds of tiny tasks, leading to massive JVM scheduling overhead and tiny I/O chunks.
* **The AQE Solution:** After shuffle stages complete, AQE inspects actual partition sizes and automatically coalesces adjacent small partitions into larger, optimal partitions.

## 3.2 Dynamic Join Strategy Conversion

* **The Problem:** Static planners choose Sort-Merge Join (SMJ) when table sizes are unknown or estimated to be large. If an intermediate table turns out to be small after filtering or aggregation, SMJ remains unnecessarily expensive.
* **The AQE Solution:** Once the build-side shuffle stage finishes, AQE checks its actual byte size. If it falls below `spark.sql.autoBroadcastJoinThreshold`, AQE dynamically injects a `BroadcastExchange`, converting SMJ into an ultra-fast Broadcast Hash Join (BHJ).

## 3.3 Dynamic Skew Join Optimization

* **The Problem:** Data skew causes straggler tasks where a single partition receives 90% of the data while others finish instantly.
* **The AQE Solution:** AQE identifies skewed partitions exceeding median size thresholds and splits them into multiple smaller sub-tasks, processing them in parallel.

---

# 4. Adaptive Execution Plan Evidence

When reviewing `explain(true)` output with AQE enabled, Spark explicitly wraps stages in `AdaptiveSparkPlan` and displays runtime optimization transitions:

```text
== Adaptive Spark Plan ==
== Physical Plan ==
SortMergeJoin [...]
+- AQEShuffleRead coalesced
   +- ShuffleQueryStage 0 (Exchange hashpartitioning(...))

```

The presence of `AQEShuffleRead` indicates that AQE has intercepted the shuffle output and applied dynamic coalescing based on runtime metrics.

---

# 5. Engineering Lessons

## 5.1 Runtime Statistics Trump Static Estimates

Catalog statistics are frequently missing or outdated for derived DataFrames. AQE bridges this gap by gathering exact byte and row counts directly from shuffle map outputs.

## 5.2 Elimination of Tuning Guesswork

AQE drastically reduces the need for manual partition tuning. Configuring a safe upper bound for shuffle partitions no longer penalizes smaller queries thanks to automatic coalescing.

---

# 6. Enterprise Use Cases for AQE

1. **Self-Optimizing Data Pipelines:** Complex enterprise ETL jobs with unpredictable user filters automatically adjust join strategies and partition counts without manual intervention.
2. **Mitigating Data Skew:** Automated skew handling prevents job failures and stragglers caused by uneven enterprise key distributions (e.g., null keys or high-frequency tenants).

---

# 7. Interview Questions

### Q1. What is Adaptive Query Execution (AQE) in Spark?

It is a dynamic optimization framework in Spark 3.x that re-optimizes physical execution plans mid-query at stage boundaries using actual runtime shuffle statistics.

---

### Q2. What are the three main features of AQE?

1. Dynamic Partition Coalescing
2. Dynamic Join Strategy Conversion (e.g., SMJ to BHJ)
3. Dynamic Skew Join Optimization

---

### Q3. How does AQE solve the data skew problem?

It detects shuffle partitions that are significantly larger than the median partition size and splits them into smaller sub-tasks to be processed concurrently.

---

# 8. Key Takeaways

1. **AQE removes the dependency on static, inaccurate compile-time statistics.**
2. **Partition coalescing eliminates task scheduling overhead for small intermediate datasets.**
3. **Dynamic join conversion turns costly shuffles into efficient broadcasts at runtime.**

---

# 9. Experiment Status

**Module 2.4 — Adaptive Query Execution (AQE) Runtime Re-Optimization: COMPLETE ✅**

Evidence collected:

* AQE session configuration and dynamic execution verification.
* Shuffle stage statistics collection and adaptive plan inspection.

### ▶️ How to Run the Module 2.4 Experiment:
```bash
./gradlew runAQEExperiment
```

### Phase 2 Complete 🚀

You have successfully completed **Phase 2 (Internals)**, mastering Catalyst (2.1), Tungsten (2.2), WholeStageCodegen (2.3), and AQE (2.4)!