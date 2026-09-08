# Module 1.5.10 — Production Partition Sizing Strategy

## Engineering Question

How should a Spark engineer choose an appropriate shuffle partition count based on data volume, cluster parallelism, workload characteristics, and runtime behavior?

---

## 1. Production Use Case

Consider a large-scale transaction processing pipeline:

```text
Transactions
     |
     v
Customer-level aggregation
     |
     +--> total transaction amount
     |
     +--> transaction count
     |
     v
Customer-level result
```

A representative Spark implementation is:

```scala
transactions
  .groupBy("customer_id")
  .agg(
    sum("amount").alias("total_amount"),
    count("*").alias("transaction_count")
  )
```

At production scale, this aggregation introduces a shuffle because records belonging to the same customer must be brought together.

The engineering question is:

> How many shuffle partitions should the aggregation use?

Too few partitions can create large partitions and insufficient parallelism.

Too many partitions can increase scheduling, shuffle metadata, task, serialization, and coordination overhead.

There is no universally correct value.

The appropriate value depends on the workload and execution environment.

---

## 2. The Problem

A common Spark configuration is:

```text
spark.sql.shuffle.partitions = 200
```

because it is Spark's default.

Using a default value without understanding the workload can lead to inefficient execution.

For example:

```text
Too few partitions

Large partition
       |
       +--> longer task
       +--> less parallelism
       +--> possible memory pressure
       +--> possible spill
```

while:

```text
Too many partitions

Small partitions
       |
       +--> more tasks
       +--> scheduling overhead
       +--> more shuffle metadata
       +--> more task startup overhead
```

The goal is not to maximize or minimize partition count.

The goal is to find a configuration where:

- partitions are large enough to amortize overhead
- partitions are small enough to process efficiently
- available cluster parallelism is utilized
- shuffle data is distributed reasonably
- skew does not create stragglers
- memory and spill behavior remain healthy

---

## 3. Starting Partition-Sizing Heuristic

A useful starting point is:

```text
partitionCount ≈ max(
    clusterParallelism,
    estimatedDataSize / targetPartitionSize
)
```

This is a sizing heuristic, not a Spark rule.

The result must be validated using actual execution metrics.

A production engineer should inspect:

- task input size
- shuffle read
- shuffle write
- task duration
- task duration distribution
- executor memory pressure
- CPU utilization
- spill
- skew
- number of output partitions
- AQE behavior

The benchmark in this module demonstrates why empirical validation matters.

---

## 4. Important Spark Partitioning Concepts

### 4.1 `defaultParallelism`

For this experiment:

```text
spark.master = local[2]
defaultParallelism = 2
```

This represents the available execution parallelism in the local environment.

It does **not** mean that every shuffle should use two partitions.

### 4.2 `spark.sql.shuffle.partitions`

This controls the initial number of partitions used for many SQL/DataFrame shuffle operations.

Candidate values in this benchmark:

```text
2, 4, 8, 16, 32, 64
```

### 4.3 AQE

Adaptive Query Execution can use runtime statistics to modify aspects of execution after shuffle statistics become available.

For the final benchmark:

```text
spark.sql.adaptive.enabled = true
spark.sql.adaptive.coalescePartitions.enabled = true
spark.sql.adaptive.advisoryPartitionSizeInBytes = 1048576
```

The advisory size is 1 MiB.

---

## 5. Experimental Setup

### Environment

```text
Spark          3.5.1
Java           17.0.20.1
OS             Windows
Spark master   local[2]
```

This is a local development benchmark, not a production cluster benchmark.

### Dataset

```text
Rows                  = 10,000,000
Distinct customers    = 1,000,000
```

The deterministic transaction-style dataset contains:

```text
transaction_id
customer_id
city
amount
```

The customer identifier is generated using:

```scala
pmod(id, CustomerCount) + 1
```

---

## 6. Dataset Caching

The input dataset is cached and materialized before benchmarking so that dataset generation is excluded from scenario timings.

The final benchmark reported:

```text
Dataset materialization time = 4.87 seconds
```

This time is not part of the partition-count comparison.

---

## 7. Benchmark Methodology

Every scenario executes the same aggregation:

```scala
transactions
  .groupBy(col("customer_id"))
  .agg(
    sum("amount").alias("total_amount"),
    count("*").alias("transaction_count")
  )
```

Candidate shuffle partition counts:

```text
2, 4, 8, 16, 32, 64
```

Each configuration uses:

```text
1 warm-up run
5 measured runs
```

The warm-up is excluded from statistics.

The primary metric is median execution time. Median is more robust than a single local run against JVM activity, operating-system scheduling, garbage collection, and background activity.

Every run validates the expected result count of 1,000,000 customer rows.

---

## 8. AQE Configuration

The final benchmark explicitly enables:

```scala
spark.conf.set("spark.sql.adaptive.enabled", true)
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", true)
spark.conf.set("spark.sql.adaptive.advisoryPartitionSizeInBytes", 1 * 1024 * 1024)
```

AQE provides runtime adaptability in addition to the configured initial shuffle partition count.

---

## 9. AQE-OFF Baseline

A controlled benchmark was executed with AQE disabled.

| Shuffle partitions | Median execution time |
|---:|---:|
| 2 | 1599 ms |
| 4 | 1578 ms |
| 8 | 1520 ms |
| **16** | **1503 ms** |
| 32 | 1552 ms |
| 64 | 1724 ms |

The best AQE-OFF configuration was 16 partitions at 1503 ms median.

Performance generally improved from 2 → 4 → 8 → 16, then degraded at 32 and 64.

This demonstrates that increasing shuffle partition count does not automatically improve performance.

---

## 10. Final AQE-ON Benchmark

The final cleaned benchmark was executed with AQE and shuffle partition coalescing enabled.

| Shuffle partitions | Min (ms) | Median (ms) | Max (ms) |
|---:|---:|---:|---:|
| 2 | 1505 | 1561 | 1637 |
| 4 | 1492 | 1507 | 1561 |
| 8 | 1427 | 1454 | 1470 |
| **16** | **1435** | **1443** | **1453** |
| 32 | 1463 | 1487 | 1509 |
| 64 | 1475 | 1500 | 1541 |

Every scenario returned 1,000,000 result rows.

The fastest configuration by median was:

```text
16 shuffle partitions
Median execution time = 1443 ms
```

---

## 11. Why 16 Partitions Won

The workload benefited from additional partitioning up to a point.

At low partition counts, the amount of work assigned to each shuffle partition is relatively large. Increasing partition count provides more granular work units.

Performance improved through 8 and 16 partitions.

Beyond that point, additional partitioning introduced enough overhead that execution became slower:

```text
16 partitions → 1443 ms
32 partitions → 1487 ms
64 partitions → 1500 ms
```

Therefore, 16 was the best measured configuration for this workload and environment.

The conclusion is **not** that 16 is universally optimal.

> The optimal shuffle partition count is workload- and environment-dependent and should be validated empirically.

---

## 12. AQE-OFF vs AQE-ON

| Partitions | AQE OFF | AQE ON | AQE ON vs OFF |
|---:|---:|---:|---:|
| 2 | 1599 ms | 1561 ms | 2.4% faster |
| 4 | 1578 ms | 1507 ms | 4.5% faster |
| 8 | 1520 ms | 1454 ms | 4.3% faster |
| **16** | **1503 ms** | **1443 ms** | **4.0% faster** |
| 32 | 1552 ms | 1487 ms | 4.2% faster |
| 64 | 1724 ms | 1500 ms | **13.0% faster** |

This is not a controlled causal measurement of AQE's performance benefit because the runs were performed separately on a local development machine.

Nevertheless, the comparison demonstrates that AQE can provide runtime adaptability while the initial shuffle partition count still influences execution behavior.

The 64-partition case is notable: the earlier AQE-OFF median was 1724 ms, while the final AQE-ON median was 1500 ms.

---

## 13. AQE Plan Evidence

The final benchmark's executed-plan snapshot consistently contained:

```text
AdaptiveSparkPlan
```

However, Spark 3.5.1 reported:

```text
isFinalPlan=false
```

when the plan was captured through:

```scala
aggregation.queryExecution.executedPlan
```

Therefore, this experiment does **not** use that output to claim a specific final AQE partition count.

A production engineer should not infer:

```text
64 configured partitions
        ↓
exactly N final AQE partitions
```

unless the execution evidence explicitly exposes that information.

---

## 14. Direct AQE Coalescing Evidence

Module 1.5.9 independently demonstrated AQE shuffle partition coalescing.

The final physical plan contained:

```text
AQEShuffleRead
Arguments: coalesced
```

and:

```text
AdaptiveSparkPlan
Arguments: isFinalPlan=true
```

This provides direct evidence that Spark can retain the shuffle boundary while adapting how downstream shuffle data is consumed.

The important distinction is:

> AQE does not necessarily remove the shuffle.

Instead, AQE can adapt execution after runtime statistics become available.

---

## 15. `defaultParallelism` vs Shuffle Partitions

One of the most important lessons is:

```text
defaultParallelism
        ≠
spark.sql.shuffle.partitions
        ≠
final AQE partition count
```

This experiment had:

```text
defaultParallelism = 2
```

while the best shuffle configuration was:

```text
spark.sql.shuffle.partitions = 16
```

There is no contradiction.

`defaultParallelism` describes available execution parallelism in the local environment.

`spark.sql.shuffle.partitions` controls the initial partitioning of shuffle output for applicable SQL/DataFrame operations.

AQE can then adapt aspects of downstream shuffle execution using runtime statistics.

---

## 16. Why "One Partition Per Core" Is Not a Production Rule

A common oversimplification is:

> Use one partition per CPU core.

This is insufficient for real Spark workloads.

Partition sizing depends on:

- data volume
- record width
- operator complexity
- aggregation state
- join strategy
- key distribution
- cluster resources
- memory pressure
- serialization
- I/O
- AQE
- workload concurrency

Therefore:

> Cluster parallelism is a starting constraint, not the complete partition-sizing strategy.

---

## 17. Relationship With Data Skew

Partition count alone cannot solve data skew.

Suppose one customer accounts for 50% of all records. Hash partitioning can still place that hot key into one partition.

Increasing the number of partitions may not solve the fundamental problem.

Module 1.5.7 demonstrated partition imbalance and Module 1.5.8 demonstrated salting as one possible mitigation technique.

Therefore:

```text
Partition sizing
       +
Data distribution
       +
Skew analysis
```

must be considered together.

---

## 18. Repartition vs Coalesce

### `repartition()`

`repartition()` introduces an Exchange/shuffle when changing the partition layout.

```scala
df.repartition(64)
```

It can increase or decrease partition count while redistributing data.

### `coalesce()`

`coalesce()` is primarily useful for reducing partitions without introducing a shuffle in the DataFrame/Dataset execution demonstrated in this project.

```scala
df.coalesce(1)
```

However:

```scala
df.coalesce(64)
```

does not create 64-way parallelism when the input has only two partitions.

---

## 19. Why `coalesce(1)` Should Be Used Carefully

Reducing output to one partition can be useful for small final outputs, such as producing a small number of output files.

But applying:

```scala
df.coalesce(1)
```

to a large dataset can create a severe bottleneck because the entire output must be processed by a single downstream partition.

> Use single-partition output deliberately, not as a generic performance optimization.

---

## 20. Why More Shuffle Partitions Are Not Always Better

The benchmark demonstrates three regions.

### Under-partitioned

```text
2 → 4
```

Increasing the number of partitions improved performance.

### Useful partitioning range

```text
8 → 16
```

The workload achieved its best measured performance around this range.

### Over-partitioned for this workload

```text
32 → 64
```

Increasing partition count no longer helped and execution became slower.

This can happen because smaller partitions create additional overhead through:

- more tasks
- more task scheduling
- more shuffle metadata
- more partition bookkeeping
- more task startup and completion overhead

The exact point depends on the workload and cluster.

---

## 21. Production Metrics to Inspect

Partition sizing should not be based on execution time alone.

### Task input size

Look for extremely large partitions. Large input per task can indicate insufficient partitioning.

### Shuffle read

Inspect shuffle read and its distribution across tasks. Large variation can indicate skew.

### Shuffle write

Inspect shuffle write to understand how much data is being redistributed.

### Task duration

A single task significantly slower than others may indicate data skew or partition imbalance.

### Spill

Inspect:

```text
Memory Bytes Spilled
Disk Bytes Spilled
```

Large spill volumes can indicate that partitions are too large for available executor memory or that the operator requires substantial intermediate state.

### Executor utilization

Evaluate:

```text
CPU utilization
memory utilization
executor concurrency
```

---

## 22. Practical Production Workflow

```text
              Start
                |
                v
       Understand workload
                |
                v
       Estimate data volume
                |
                v
     Determine cluster parallelism
                |
                v
       Choose target partition size
                |
                v
      Calculate starting partition
              count
                |
                v
      Run representative workload
                |
                v
       Inspect Spark UI metrics
                |
        +-------+-------+
        |               |
        v               v
   Partitions too    Partitions too
      large?            small?
        |               |
        v               v
    Increase          Decrease
    partitions        partitions
        |               |
        +-------+-------+
                |
                v
          Check skew
                |
                v
          Check spills
                |
                v
        Check task duration
                |
                v
       Evaluate AQE behavior
                |
                v
        Validate at scale
```

---

## 23. Step-by-Step Production Strategy

### Step 1 — Understand the workload

Identify:

```text
input size
record size
operation type
shuffle stages
joins
aggregations
key distribution
```

### Step 2 — Understand the cluster

Identify:

```text
executor count
executor cores
executor memory
available parallelism
```

### Step 3 — Establish a starting partition count

Use:

```text
max(
    cluster parallelism,
    estimated data size / target partition size
)
```

as an initial estimate. Do not treat it as a guaranteed optimum.

### Step 4 — Enable AQE

Where appropriate:

```text
spark.sql.adaptive.enabled = true
spark.sql.adaptive.coalescePartitions.enabled = true
```

### Step 5 — Benchmark representative data

Test several nearby partition counts rather than assuming the default is optimal.

### Step 6 — Inspect Spark UI

Look for task duration, shuffle read/write, spill, skew, and partition distribution.

### Step 7 — Validate at production scale

A configuration that performs well on 10 GB may behave differently on 1 TB because partition distribution, memory pressure, shuffle volume, and cluster utilization can change.

---

## 24. Benchmarking Lessons

The first benchmark run is not sufficient evidence.

A single result does not prove that a configuration is better.

A stronger methodology is:

```text
Warm-up
+
multiple measured runs
+
median
+
correctness validation
```

This experiment uses one warm-up and five measured runs and validates 1,000,000 result rows for every scenario.

Partition tuning on an artificial dataset is useful for understanding Spark behavior, but production decisions should ultimately be validated against representative production characteristics.

Important variables include:

- data volume
- row width
- key cardinality
- key frequency distribution
- operator complexity
- cluster size
- executor configuration
- workload concurrency

---

## 25. Limitations of This Benchmark

This experiment is intentionally a local engineering benchmark.

It should not be interpreted as a production capacity test.

Environment:

```text
Windows
local[2]
single machine
```

Dataset:

```text
10M rows
1M customers
cached in memory
```

The benchmark does not reproduce:

- multi-node network shuffle
- executor-to-executor communication
- cloud storage latency
- distributed disk I/O
- executor failures
- cluster contention
- dynamic allocation
- heterogeneous executors
- production workload concurrency

The absolute timings should therefore not be used for capacity planning.

The value of the experiment is the methodology and observed relationship between partition count and execution behavior.

---

## 26. Translating the Experiment to a Real Cluster

The local benchmark uses:

```text
local[2]
```

A production cluster might have, for example:

```text
10 executors
×
4 cores
=
40 executor cores
```

A production engineer should not simply copy:


```text
spark.sql.shuffle.partitions = 16
```

from this experiment.

Instead:

1. estimate shuffle volume
2. understand available executor parallelism
3. choose a starting target partition size
4. calculate an initial partition count
5. enable AQE where appropriate
6. benchmark representative data
7. inspect Spark UI
8. adjust based on observed task and shuffle behavior

The number `16` is evidence for this benchmark, not a production configuration recommendation.

---

## 27. Production Partition-Sizing Checklist

### Data

- How much data is being shuffled?
- What is the average row width?
- How many distinct keys exist?
- Is the key distribution uniform?

### Cluster

- How many executors are available?
- How many cores per executor?
- How much executor memory is available?
- What is the effective cluster parallelism?

### Execution

- How many shuffle stages exist?
- How much data is written?
- How much data is read?
- Are tasks evenly sized?
- Are there stragglers?
- Is there memory or disk spill?

### AQE

- Is AQE enabled?
- Is shuffle partition coalescing enabled?
- Is the advisory partition size appropriate?
- What does the final executed plan show?

### Validation

- Was the workload benchmarked multiple times?
- Was a warm-up performed?
- Was median execution time used?
- Was correctness validated?
- Was the configuration tested at representative scale?

---

## 28. Key Engineering Takeaways

### 1. More partitions do not automatically mean better performance

Increasing 2 → 4 → 8 → 16 helped this workload. Increasing further to 32 and 64 made it slower.

### 2. Partition sizing is workload-specific

There is no universally correct `spark.sql.shuffle.partitions` value.

### 3. `defaultParallelism` and shuffle partition count are different concepts

This experiment had `defaultParallelism = 2`, but the best shuffle partition count was 16.

### 4. AQE adds runtime adaptability

AQE can use runtime statistics to adapt shuffle execution. It does not guarantee lower wall-clock time for every workload.

### 5. Partition count and data skew are different problems

More partitions cannot necessarily solve a highly skewed key. Skew may require salting, two-phase aggregation, join-side replication, or key redesign where appropriate.

### 6. Benchmark instead of guessing

The strongest production approach is:

```text
Estimate
   ↓
Benchmark
   ↓
Inspect Spark UI
   ↓
Tune
   ↓
Validate at scale
```

rather than simply using the default.

### 7. AQE does not replace good initial configuration

A strong production strategy is:

```text
Reasonable initial configuration
              +
AQE
              +
Spark UI validation
              +
Representative benchmarking
```

---

## 29. Final Results

For this benchmark:

```text
Spark 3.5.1
local[2]
10,000,000 rows
1,000,000 customers
AQE enabled
1 warm-up
5 measured runs
```

the final AQE-ON results were:

```text
2 partitions  -> 1561 ms median
4 partitions  -> 1507 ms median
8 partitions  -> 1454 ms median
16 partitions -> 1443 ms median  <-- best
32 partitions -> 1487 ms median
64 partitions -> 1500 ms median
```

The experimentally optimal configuration was:

```text
16 shuffle partitions
Median = 1443 ms
```

for this workload and environment.

The result should not be generalized to other clusters or workloads.

---

## 30. Final Engineering Conclusion

Spark partition sizing is not about finding one magic number.

It is about balancing:

```text
Data volume
     +
Cluster parallelism
     +
Partition size
     +
Operator characteristics
     +
Data distribution
     +
AQE
     +
Observed runtime behavior
```

The experiment demonstrated this principle with a controlled 10-million-row workload.

The best configuration in this environment was:

```text
16 shuffle partitions
Median = 1443 ms
```

But the production lesson is more important than the number:

> **Treat partition count as an experimentally validated workload parameter, not a universal constant.**

A senior Spark engineer should therefore approach partition sizing as an iterative engineering process:

```text
Understand
    ↓
Estimate
    ↓
Configure
    ↓
Benchmark
    ↓
Inspect
    ↓
Tune
    ↓
Validate
```

That is the difference between simply setting:

```text
spark.sql.shuffle.partitions = 200
```

and engineering a Spark workload for predictable production performance.
