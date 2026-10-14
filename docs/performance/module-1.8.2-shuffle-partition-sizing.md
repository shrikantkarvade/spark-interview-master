# Module 1.8.2 — Shuffle Partition Sizing

## Overview

Shuffle partition sizing is one of the most important tuning decisions in Apache Spark.

The number of shuffle partitions controls how Spark distributes data after a shuffle boundary. Too few partitions can create large tasks, high memory pressure, long-running tasks, and potentially spills. Too many partitions can increase scheduling overhead and produce unnecessarily small tasks.

However, with Adaptive Query Execution (AQE) enabled, the configured value is not necessarily the number of partitions that downstream operators ultimately process.

This experiment investigates that distinction by running the same aggregation workload with:

* 8 shuffle partitions
* 16 shuffle partitions
* 32 shuffle partitions
* 64 shuffle partitions
* 128 shuffle partitions
* 256 shuffle partitions

The experiment then inspects:

1. Physical plans
2. Shuffle write metrics
3. Shuffle read metrics
4. AQE partition coalescing
5. Hash aggregation memory
6. Aggregation build time
7. Spill behavior
8. Runtime differences

---

# 1. Objective

The objective is to understand how Spark behaves when the configured shuffle partition count changes while the workload remains constant.

Specifically, this experiment answers:

> **Does increasing `spark.sql.shuffle.partitions` automatically increase the number of partitions processed by downstream operators?**

And:

> **How does AQE change the effective shuffle parallelism?**

---

# 2. Experiment Configuration

All six runs use the same deterministic dataset.

| Parameter                 |                   Value |
| ------------------------- | ----------------------: |
| Input rows                |              10,000,000 |
| Distinct customers        |               1,000,000 |
| Input partitions          |                       6 |
| Shuffle partitions tested | 8, 16, 32, 64, 128, 256 |
| Aggregation key           |            `customerId` |
| AQE                       |                 Enabled |
| Execution mode            |                   Local |
| Spark version             |                   3.5.1 |

The input is generated using:

```scala
spark.range(
  start = 0,
  end = 10000000,
  step = 1,
  numPartitions = 6
)
```

The customer ID is generated deterministically:

```text
customerId = id % 1,000,000
```

The aggregation is:

```scala
input
  .groupBy("customerId")
  .count()
```

---

# 3. Why This Dataset Is Useful

The workload contains:

* 10 million input rows
* 1 million distinct grouping keys
* 6 input partitions

Each input partition covers the complete `customerId` range because of the deterministic modulo expression.

Therefore, each input partition can produce approximately 1 million unique customer IDs during the local pre-shuffle aggregation.

With six input partitions, the first aggregation produces approximately:

```text
6 × 1,000,000
= 6,000,000 records
```

before the shuffle.

This is specific to this deterministic benchmark and should **not** be interpreted as a universal property of Spark aggregations.

---

# 4. Physical Execution Pattern

The important portion of the physical plan is:

```text
Range
  ↓
Project
  ↓
HashAggregate
  ↓
Exchange
  ↓
ShuffleQueryStage
  ↓
AQEShuffleRead
  ↓
HashAggregate
```

For example, with 128 configured shuffle partitions:

```text
Exchange
Arguments: hashpartitioning(customerId#2L, 128)

ShuffleQueryStage

AQEShuffleRead
Arguments: coalesced
```

This demonstrates an important distinction:

```text
Configured Shuffle Partitions
          ↓
      Exchange
          ↓
  Shuffle materialization
          ↓
        AQE
          ↓
 Coalesced Shuffle Read
          ↓
Downstream processing
```

The configured value determines the shuffle partitioning at the Exchange.

AQE can subsequently coalesce those partitions for downstream processing when the resulting partition sizes are considered suitable.

---

# 5. Experimental Matrix

| Configured Shuffle Partitions | AQE Coalesced Partitions | Query Duration |
| ----------------------------: | -----------------------: | -------------: |
|                             8 |                        2 |            4 s |
|                            16 |                        2 |            7 s |
|                            32 |                        2 |            4 s |
|                            64 |                        2 |            4 s |
|                           128 |                        2 |            4 s |
|                           256 |                        2 |            5 s |

## Important Interpretation

The most important observation is:

> **All six configurations were reduced to 2 downstream AQE partitions.**

Therefore, this experiment does **not** show six different levels of downstream parallelism.

Instead, it demonstrates that:

```text
8   → AQE → 2
16  → AQE → 2
32  → AQE → 2
64  → AQE → 2
128 → AQE → 2
256 → AQE → 2
```

The configured partition count still matters because it controls the shuffle Exchange, but AQE subsequently changes the effective partitioning seen by downstream operators.

---

# 6. Shuffle Data Comparison

| Shuffle Partitions | Shuffle Records | Data Size | Shuffle Bytes Written | Local Bytes Read |
| -----------------: | --------------: | --------: | --------------------: | ---------------: |
|                  8 |       6,000,000 |  91.6 MiB |              28.8 MiB |         28.8 MiB |
|                 16 |       6,000,000 |  91.6 MiB |              28.9 MiB |         28.9 MiB |
|                 32 |       6,000,000 |  91.6 MiB |              29.0 MiB |         29.0 MiB |
|                 64 |       6,000,000 |  91.6 MiB |              29.2 MiB |         29.2 MiB |
|                128 |       6,000,000 |  91.6 MiB |              29.7 MiB |         29.7 MiB |
|                256 |       6,000,000 |  91.6 MiB |              30.1 MiB |         30.1 MiB |

The logical workload remains effectively constant:

```text
6,000,000 shuffle records
91.6 MiB logical shuffle data
```

The serialized shuffle bytes vary slightly between runs.

The `data size` and `shuffle bytes written` metrics should not be treated as identical measurements. They represent different stages of Spark's data representation and serialization/compression path.

---

# 7. AQE Coalescing

AQE reported:

```text
number of partitions: 2
number of coalesced partitions: 2
```

for every configuration tested.

The resulting partition sizes were approximately 15–16 MiB each.

Examples:

### 8 partitions

```text
AQE partitions: 2
Partition data size: 31.4 MiB total
Maximum: 15.7 MiB
```

### 32 partitions

```text
AQE partitions: 2
Partition data size: 30.1 MiB total
Maximum: 15.0 MiB
```

### 128 partitions

```text
AQE partitions: 2
Partition data size: 31.6 MiB total
Maximum: 16.0 MiB
```

### 256 partitions

```text
AQE partitions: 2
Partition data size: 31.9 MiB total
Maximum: 16.0 MiB
```

This is strong evidence that AQE is actively adapting the downstream partition layout instead of blindly preserving the configured partition count.

---

# 8. Aggregation Memory

The first HashAggregate consistently reported approximately:

```text
Peak memory per task: 96 MiB
Total peak memory: 576 MiB
```

This is consistent with the six input tasks participating in the first local aggregation.

For example, the 128-partition run reported:

```text
peak memory total:
576.0 MiB

min: 0.0 B
median: 96.0 MiB
max: 96.0 MiB
max task: Stage 0, Task 0
```

The downstream aggregation reported approximately:

```text
Peak memory per task: 80 MiB
Total: 160 MiB
```

because AQE reduced the downstream processing to two partitions.

For example:

```text
128 configured partitions
        ↓
AQE
        ↓
2 downstream partitions
        ↓
~80 MiB peak memory per task
```

This illustrates why configured shuffle partitions and effective downstream task parallelism should be analyzed separately.

---

# 9. Spill Behavior

All six runs reported:

```text
Spill size: 0 B
```

and:

```text
Number of sort fallback tasks: 0
```

Therefore, this workload did not create enough aggregation memory pressure to trigger spilling or sort fallback.

This is an important benchmark limitation.

The absence of spill does **not** mean that partition sizing never causes memory problems. A larger production dataset, wider rows, larger aggregation state, or skewed keys could produce very different behavior.

---

# 10. Aggregation Build Time

The first HashAggregate build times were approximately:

| Shuffle Partitions | Aggregation Build Time |
| -----------------: | ---------------------: |
|                  8 |                  2.0 s |
|                 16 |                  3.1 s |
|                 32 |                  2.2 s |
|                 64 |                  2.1 s |
|                128 |                  2.1 s |
|                256 |                  2.2 s |

The 16-partition run showed a higher local aggregation time than the other runs.

However, this should not be interpreted as proof that 16 partitions are inherently slower.

These runs were short local executions, where JVM scheduling, JIT compilation, operating-system scheduling, and other runtime effects can materially influence measurements.

The experiment therefore provides stronger evidence about **execution structure and AQE behavior** than about a universal runtime-optimal partition count.

---

# 11. Runtime Comparison

Observed query durations:

| Shuffle Partitions | Duration |
| -----------------: | -------: |
|                  8 |      4 s |
|                 16 |      7 s |
|                 32 |      4 s |
|                 64 |      4 s |
|                128 |      4 s |
|                256 |      5 s |

There is no monotonic relationship between the configured partition count and observed runtime in this local benchmark.

For example:

```text
8   → 4 s
16  → 7 s
32  → 4 s
64  → 4 s
128 → 4 s
256 → 5 s
```

This reinforces an important engineering principle:

> **Do not select a production shuffle partition count solely because one short benchmark run produced the lowest elapsed time.**

A production tuning decision should consider:

* shuffle volume
* partition size
* task duration
* executor memory
* spill
* skew
* scheduler overhead
* cluster size
* workload concurrency
* AQE behavior
* stability across repeated runs

---

# 12. Stage and Task Evidence

## 8 Partitions

Important maximum metrics included:

```text
WholeStageCodegen (2)
Max: 796 ms
Stage 2, Task 6

WholeStageCodegen (1)
Max: 778 ms
Stage 0, Task 0

Shuffle write time
Max: 27 ms
Stage 0, Task 1

Aggregation build time
Max: 410 ms
Stage 0, Task 0
```

---

## 16 Partitions

```text
WholeStageCodegen (2)
Max: 1.2 s
Stage 2, Task 6

WholeStageCodegen (1)
Max: 1.3 s
Stage 0, Task 0

Shuffle write time
Max: 69 ms
Stage 0, Task 1

Aggregation build time
Max: 587 ms
Stage 0, Task 5
```

---

## 32 Partitions

```text
WholeStageCodegen (2)
Max: 870 ms
Stage 2, Task 6

WholeStageCodegen (1)
Max: 864 ms
Stage 0, Task 0

Shuffle write time
Max: 52 ms
Stage 0, Task 1

Aggregation build time
Max: 480 ms
Stage 0, Task 1
```

---

## 64 Partitions

```text
WholeStageCodegen (2)
Max: 819 ms
Stage 2, Task 6

WholeStageCodegen (1)
Max: 881 ms
Stage 0, Task 1

Shuffle write time
Max: 69 ms
Stage 0, Task 0

Aggregation build time
Max: 468 ms
Stage 0, Task 1
```

---

## 128 Partitions

```text
WholeStageCodegen (2)
Max: 903 ms
Stage 2, Task 7

WholeStageCodegen (1)
Max: 840 ms
Stage 0, Task 0

Shuffle write time
Max: 128 ms
Stage 0, Task 0

Aggregation build time
Max: 449 ms
Stage 0, Task 1
```

---

## 256 Partitions

```text
WholeStageCodegen (2)
Max: 883 ms
Stage 2, Task 7

WholeStageCodegen (1)
Max: 847 ms
Stage 0, Task 0

Shuffle write time
Max: 9 ms
Stage 0, Task 5

Aggregation build time
Max: 471 ms
Stage 0, Task 0
```

The Stage/Task maximums are useful when diagnosing individual task behavior, but they should not be interpreted as stable performance characteristics from a single local execution.

---

# 13. What Does `spark.sql.shuffle.partitions` Actually Control?

A common misconception is:

```text
spark.sql.shuffle.partitions = 256
```

means:

```text
256 downstream tasks will definitely execute.
```

That is not necessarily true when AQE is enabled.

The experiment demonstrates:

```text
spark.sql.shuffle.partitions = 256
             ↓
Exchange creates 256-way hash partitioning
             ↓
ShuffleQueryStage
             ↓
AQE analyzes shuffle statistics
             ↓
AQEShuffleRead coalesces partitions
             ↓
2 downstream partitions
```

Therefore:

> **The configured shuffle partition count controls the initial shuffle partitioning, while AQE can change the effective downstream partition layout.**

---

# 14. Why Too Few Shuffle Partitions Can Be Dangerous

If a workload produces a large amount of shuffle data and too few partitions are configured, each partition can become large.

This can lead to:

* Large task input
* Higher executor memory pressure
* Larger aggregation state
* More spill
* Longer task duration
* Greater impact from skewed partitions
* Potential executor instability

Conceptually:

```text
Large Shuffle
     ↓
Too Few Partitions
     ↓
Large Partitions
     ↓
Large Tasks
     ↓
Memory Pressure / Spill
     ↓
Longer Execution
```

---

# 15. Why Too Many Shuffle Partitions Can Also Be Harmful

The opposite extreme can create many small shuffle partitions.

Potential consequences include:

* More task scheduling overhead
* More task startup overhead
* More shuffle metadata
* More shuffle files/blocks
* Smaller amounts of useful work per task
* Increased scheduler pressure

Conceptually:

```text
Large Shuffle
     ↓
Too Many Partitions
     ↓
Many Small Tasks
     ↓
Scheduling / Task Overhead
     ↓
Potentially Inefficient Execution
```

The optimal configuration is therefore workload-dependent.

---

# 16. AQE Changes the Tuning Equation

With AQE enabled, partition tuning is no longer simply:

```text
Choose the perfect static partition count
```

Instead:

```text
Choose a reasonable initial partitioning
                ↓
Let AQE inspect runtime statistics
                ↓
AQE coalesces small partitions
                ↓
AQE adapts downstream execution
```

This does not eliminate the need for partition tuning.

The initial partition count still affects:

* Shuffle partitioning
* Shuffle file/block structure
* Data distribution
* Initial task layout
* Amount of data available for AQE to coalesce

AQE is an optimization mechanism, not a replacement for understanding shuffle sizing.

---

# 17. Production Engineering Perspective

In production, shuffle partition sizing should be based on observed workload characteristics rather than a fixed universal number.

Useful measurements include:

### Shuffle volume

```text
Total shuffle bytes
```

### Partition size

```text
Average partition size
Maximum partition size
Partition-size distribution
```

### Task behavior

```text
Task duration
Input records
Shuffle read
Shuffle write
```

### Memory behavior

```text
Peak execution memory
Spill
GC time
Executor memory pressure
```

### Distribution

```text
Partition-size skew
Hot keys
Long-tail tasks
```

### AQE behavior

```text
Initial partitions
Coalesced partitions
Post-AQE partition sizes
```

The objective is not:

> "Use 128 partitions."

The objective is:

> "Produce partitions that are appropriately sized for the workload and cluster while avoiding excessive task overhead."

---

# 18. Practical Diagnostic Workflow

When a Spark aggregation is slow in production, use a systematic workflow.

```text
1. Measure shuffle volume
        ↓
2. Inspect partition sizes
        ↓
3. Check task-duration distribution
        ↓
4. Check aggregation memory
        ↓
5. Check spill
        ↓
6. Check skew
        ↓
7. Inspect physical plan
        ↓
8. Check AQE behavior
        ↓
9. Adjust partitioning/configuration
        ↓
10. Re-run and compare
```

Do not immediately increase:

```text
spark.sql.shuffle.partitions
```

without understanding the underlying problem.

---

# 19. Key Findings

### Finding 1 — Configured partitions are not necessarily effective downstream partitions

All six configurations:

```text
8, 16, 32, 64, 128, 256
```

were reduced by AQE to:

```text
2 downstream partitions
```

---

### Finding 2 — Shuffle volume remained approximately constant

Every run processed:

```text
6,000,000 shuffle records
91.6 MiB logical shuffle data
```

because the underlying workload did not change.

---

### Finding 3 — AQE coalescing was clearly visible

The physical plans contained:

```text
AQEShuffleRead
Arguments: coalesced
```

This is direct physical-plan evidence that AQE modified the shuffle read behavior.

---

### Finding 4 — No spill occurred

All configurations reported:

```text
Spill = 0 B
Sort fallback = 0 tasks
```

The tested workload therefore did not experience aggregation memory pressure severe enough to spill.

---

### Finding 5 — Runtime did not scale monotonically with partition count

Observed runtimes ranged from:

```text
4–7 seconds
```

with no monotonic relationship to configured partition count.

The 16-partition run took 7 seconds, while several other configurations completed in 4 seconds.

This difference should be treated as local runtime variability rather than proof of a universally inferior configuration.

---

# 20. Common Interview Question

## Does setting `spark.sql.shuffle.partitions=256` guarantee 256 tasks after the shuffle?

**No.**

The configuration establishes the initial shuffle partitioning.

With AQE enabled, Spark can inspect runtime shuffle statistics and coalesce partitions for downstream processing.

This experiment demonstrated:

```text
Configured: 256
        ↓
Exchange: hashpartitioning(..., 256)
        ↓
AQE
        ↓
Downstream: 2 coalesced partitions
```

The exact behavior depends on the workload and AQE configuration.

---

# 21. Another Interview Question

## Should we always increase `spark.sql.shuffle.partitions` when a Spark job is slow?

**No.**

First determine why the job is slow.

If partitions are too large:

```text
Increase parallelism
```

may help.

If partitions are already small:

```text
Increase partition count
```

may simply create more overhead.

If the real problem is skew:

```text
Partition count alone
```

may not solve it.

If aggregation state is large:

```text
Memory / aggregation strategy
```

may be the more important issue.

If AQE is already coalescing partitions:

```text
Changing the static value
```

may have less downstream impact than expected.

---

# 22. Production Optimization Checklist

Before changing shuffle partition configuration, check:

* [ ] Total shuffle data
* [ ] Number of shuffle records
* [ ] Average partition size
* [ ] Maximum partition size
* [ ] Task-duration distribution
* [ ] Long-running tasks
* [ ] Aggregation memory
* [ ] Spill
* [ ] GC time
* [ ] Data skew
* [ ] AQE enabled/disabled
* [ ] AQE coalescing behavior
* [ ] Cluster executor count
* [ ] Executor cores
* [ ] Workload concurrency
* [ ] Physical execution plan

---

# 23. Final Takeaway

Shuffle partition sizing is not about finding one magic number.

The engineering objective is to balance:

```text
Partition Size
        +
Parallelism
        +
Memory
        +
Scheduler Overhead
        +
Data Distribution
        +
AQE Behavior
```

This experiment demonstrated an especially important Spark concept:

```text
Configured Shuffle Partitions
            ≠
Effective Downstream Partitions
```

With AQE enabled, Spark can transform:

```text
8   → 2
16  → 2
32  → 2
64  → 2
128 → 2
256 → 2
```

for this workload.

Therefore, production tuning should focus on **observed partition sizes, task behavior, shuffle volume, memory pressure, skew, and AQE decisions**, rather than relying on a fixed partition-count rule.

---

# 24. Experiment Status

**Module 1.8.2 — Shuffle Partition Sizing: COMPLETE**

Test matrix completed:

```text
8
16
32
64
128
256
```

Evidence collected from:

* Spark Physical Plans
* Spark SQL UI
* Shuffle metrics
* AQE metrics
* HashAggregate metrics
* Stage/Task maximums
* Runtime measurements

### Next Module

**Module 1.8.3 — Shuffle Read/Write Performance**

The next experiment should move beyond partition count and investigate the mechanics and performance implications of shuffle read/write behavior, including how shuffle volume, partition distribution, serialization, and task-level processing affect execution.
