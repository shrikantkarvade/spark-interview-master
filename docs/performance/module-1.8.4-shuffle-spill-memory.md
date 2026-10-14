# Module 1.8.4 — Shuffle Spill & Memory Engineering

## 1. Executive Summary

Module 1.8.4 investigates what happens when Spark shuffle and aggregation workloads exceed available execution memory. Building directly upon **Module 1.8.3**—which established a baseline of $0\text{ B}$ spill under ample memory conditions—this module forces deterministic memory pressure to study how Spark's Tungsten aggregation engine, `UnsafeExternalSorter`, and `RowBasedKeyValueBatch` handle memory constraints through graceful disk spilling rather than hard `OutOfMemoryError` failures.

Through a controlled six-case experimental matrix (Cases A through F), we examine how heap size restrictions, memory fractions, aggregation key-space width, partition counts, and Adaptive Query Execution (AQE) interact with execution memory.

---

## 2. Spark Memory Architecture & Experimental Setup

### Memory Pressure Configuration

Spark's execution memory pool is governed by JVM heap size (`spark.executor.memory` / driver heap) and `spark.memory.fraction` (default `0.60`). Under default local test environments (~3.9 GiB heap), execution memory is too large for moderate datasets to naturally pressure aggregation buffers.

To create controlled, repeatable spill behavior without failing the JVM, Module 1.8.4 introduces:

1. **Scoped Task Heap Size:** `-Xmx512m` restricted specifically to the experiment Gradle task.
2. **Clamped Memory Fraction:** `spark.memory.fraction = 0.05`, shrinking the execution/storage pool down to an active execution limit of $\approx 8.3\text{ MiB}$.
3. **Isolated JVMs:** Each case runs as an independent process because `spark.memory.fraction` and heap allocations are immutable post-`SparkContext` initialization.

### Fixed Dataset Baseline

* **Input Rows:** $10,000,000$
* **Distinct Keys:** $1,000,000$ (`customer_id = id % 1,000,000`, yielding $\approx 10$ rows per key on average)
* **Input Partitions:** $6$
* **Input Caching:** Removed (`input.cache()` was eliminated after diagnosing that pinned cached blocks starved execution memory and prevented Spark's spill-to-disk fallback from initializing).

---

## 3. The Experimental Matrix (Cases A–F)

| Case | Configuration / Variable | Execution Behavior & Core Finding |
| --- | --- | --- |
| **A** | Baseline (Ample Memory, default fraction) | **Control confirmed:** $0\text{ B}$ spill. Replicates Module 1.8.3 behavior when memory headroom is unconstrained. |
| **B** | Constrained Memory (`0.05`), Single-Key Aggregation ($24$ Partitions) | **Successful graceful spill:** Total HashAggregate spill of **$655.5\text{ MiB}$**, max task spill of **$109.2\text{ MiB}$** (Stage 0 / Task 1), 6 sort fallback tasks, successful job completion. |
| **C** | Constrained Memory, Wide Aggregation ($2$-key grouping, $24$ Partitions) | Expanding the grouping key space (`customer_id, product_id`) increases aggregation state width, accelerating memory pressure and continuous disk spilling. |
| **D** | Constrained Memory, Few Partitions ($6$ Shuffle Partitions) | Evaluates how larger per-task input payloads impact aggregation memory allocation under tight execution limits. |
| **E** | Constrained Memory, Many Partitions ($64$ Shuffle Partitions) | Evaluates how fracturing input into smaller per-task chunks alters memory pressure and task-level spill distribution. |
| **F** | Constrained Memory, AQE Enabled ($64$ Partitions, AQE = true) | Evaluates whether dynamic partition coalescing interacts with or concentrates spill patterns under severe memory constraint. |

---

## 4. Deep-Dive Analysis: Case B Metrics & Physical Execution

Case B provides the primary quantitative benchmark for graceful spill behavior.

### Physical Plan Structure

```text
== Physical Plan ==
- HashAggregate (Final/Partial)
+- Exchange (hashpartitioning, 24)
   +- * HashAggregate (Partial)
      +- * Project (customer_id = id % 1,000,000)
         +- * Range (0 to 10M, 6 splits)

```

### Key Performance Evidence (Case B)

* **Total HashAggregate Spill:** $655.5\text{ MiB}$
* **Max Task Spill:** $109.2\text{ MiB}$ (Stage 0, Task 1)
* **Peak Aggregation Memory (Total):** $54.0\text{ MiB}$
* **Peak Aggregation Memory (Max Task):** $9.0\text{ MiB}$ (Stage 0, Task 1)
* **Sort Fallback Tasks:** $6$ (All upstream tasks triggered sort-based aggregation fallback)
* **Final Shuffle Write:** $28.9\text{ MiB}$

### Critical Insight: Spill vs. Shuffle Volume

One of the most profound takeaways from Case B is the ratio between disk spill and final shuffle output:


$$\text{Spill } (655.5\text{ MiB}) \gg \text{ Shuffle Output } (28.9\text{ MiB})$$

This demonstrates that memory pressure occurred *inside* the upstream aggregation buffer while building hash tables over $10\text{M}$ rows. Rather than bloating the network shuffle, Tungsten repeatedly sorted and flushed intermediate aggregation state to disk, ultimately condensing the dataset into a compact $28.9\text{ MiB}$ shuffle payload.

### Task-Level Skew Analysis

Spark UI metrics for Case B revealed a non-uniform task distribution:

* **Min Spill:** $0.0\text{ B}$
* **Median Spill:** $0.0\text{ B}$
* **Max Spill:** $109.2\text{ MiB}$ (Stage 0, Task 1)

This proves that memory pressure and spill are highly task-dependent. Even with deterministically generated data (`spark.range`), individual tasks accumulated disparate state sizes before triggering spill thresholds, reinforcing that task-level metrics must be analyzed via percentiles rather than aggregate totals alone.

---

## 5. Production Implications & Mitigation Guidelines

1. **Recognize Spill in the Spark UI:**
* Look for non-zero **"Spill (Memory)"** and **"Spill (Disk)"** metrics in the Stages tab. High spill indicates that execution memory (`spark.memory.fraction`) is insufficient for the active shuffle/aggregation buffers.


2. **Tune Memory Fractions Carefully:**
* If memory spill degrades job performance due to excessive disk I/O, adjust `spark.memory.fraction` or increase executor memory. However, remember that increasing execution memory reduces storage memory available for caching.


3. **Optimize Aggregation State Width:**
* As seen in Case C, widening grouping keys inflates the memory footprint per entry in `BytesToBytesMap`. Reducing unnecessary columns in early projections directly lowers memory pressure.


4. **Partition Sizing & AQE:**
* Aligning shuffle partitions (`spark.sql.shuffle.partitions`) ensures that per-task memory buffers remain balanced, preventing localized hotspot tasks from shoulder-checking memory limits while other tasks remain idle.