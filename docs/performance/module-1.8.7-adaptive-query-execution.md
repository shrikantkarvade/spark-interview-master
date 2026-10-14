# Module 1.8.7 — Adaptive Query Execution (AQE) Shuffle Optimizations

## Overview

This module explores **Adaptive Query Execution (AQE)**, a core optimization framework introduced in **Spark 3.0** that re-optimizes physical execution plans dynamically at runtime using exact statistics gathered during shuffle completion.

In traditional Spark execution, physical plans are generated statically before execution begins based on catalog statistics. AQE bridges this gap by introducing runtime feedback loops between shuffle stages.

---

## The Core Limitation: Static Execution Planning

Static query planning creates several well-known performance bottlenecks in distributed data processing:

1. **Suboptimal Partition Counts:** Specifying a high static count like `spark.sql.shuffle.partitions = 200` for a small or moderately sized dataset results in hundreds of tiny tasks, causing excessive task scheduling overhead and small file I/O storms.
2. **Data Skew:** A single heavy key can direct an overwhelming percentage of a partition's records to a single reducer task while others finish instantly, creating a severe straggler bottleneck.
3. **Inaccurate Join Strategies:** Initial size estimates may incorrectly classify a table as too large for a broadcast join, even though runtime filters shrink it well below the broadcast threshold.

---

## The Three Pillars of AQE

AQE addresses these limitations by dynamically adjusting the execution plan at shuffle stage boundaries:

```text
Standard Spark:
Query ──> Static Plan ──> Execution (No adjustments based on runtime stats)

Adaptive Query Execution (AQE):
Query ──> Initial Plan ──> Shuffle Stage 1 Complete ──[Runtime Stats]──> Re-optimized Plan ──> Final Stages

```

1. **Dynamic Partition Coalescing (`spark.sql.adaptive.coalescePartitions.enabled`):**
* Automatically combines adjacent small shuffle partitions into larger ones based on an advisory target size, eliminating task scheduling overhead.


2. **Dynamic Skew Join Optimization (`spark.sql.adaptive.skewJoin.enabled`):**
* Detects skewed shuffle partitions and automatically splits them into smaller sub-tasks to balance workloads across executors.


3. **Dynamic Switching of Join Strategies:**
* Converts Sort-Merge Joins into Broadcast Hash Joins on the fly if runtime statistics prove one side of the join is smaller than the broadcast threshold.



---

## Experimental Matrix

Each test case runs as an independent JVM process to ensure clean configuration binding at `SparkContext` initialization.

* **Fixed Dataset:** 10,000,000 rows, 1,000,000 distinct keys, 6 input partitions, with initial shuffle partitions set to **200**.

| Case | Label | AQE Enabled (`spark.sql.adaptive.enabled`) | Initial Shuffle Partitions | Purpose / Objective |
| --- | --- | --- | --- | --- |
| **A** | AQE DISABLED (STATIC PLANNING) | `false` | 200 | Baseline control; forces Spark to execute all 200 static shuffle partitions regardless of data volume. |
| **B** | AQE ENABLED (DYNAMIC COALESCING) | `true` | 200 | Demonstrates dynamic partition coalescing based on runtime shuffle file statistics. |

---

## Empirical Findings & Analysis

1. **Partition Coalescing in Practice:**
* In Case A, Spark scheduled and executed all **200 static partitions**, resulting in unnecessary scheduling and task initialization overhead for lightweight data blocks.
* In Case B, AQE inspected the runtime map output sizes post-shuffle (`advisory target size: 67108864`) and **dynamically coalesced the downstream shuffle stage**, drastically reducing task scheduling overhead.


2. **Performance Impact:**
* Enabling AQE prevents the classic anti-pattern of over-partitioning shuffles, protecting the cluster driver from task thrashing while maintaining parallelism where data volume actually demands it.



---