# Module 1.5.8 — Data Skew Mitigation Using Salting

## Engineering Question

> How can we split a hot key across multiple Spark partitions without losing the ability to process the data in parallel?

This experiment builds directly on Module 1.5.7, where deliberate key skew produced severe partition imbalance after hash partitioning.

## Use Case

Large transaction or event datasets are often partitioned by business keys such as customer, account, merchant, tenant, or product ID. In production, one or a few keys can become extremely hot.

For example:

```text
customer_id = 0  -> 50% of all records
customer_id = 1  -> 20%
customer_id = 2  -> 10%
```

If data is hash-partitioned by the business key, records for the same key follow the same partitioning rule. A hot key can therefore make one partition dramatically larger than the others, reducing parallelism and creating a straggler.

## Problem

Module 1.5.7 deliberately created this distribution:

| Key | Rows |
|---:|---:|
| 0 | 500,000 |
| 1 | 200,000 |
| 2 | 100,000 |
| 3 | 50,000 |
| 4 | 50,000 |
| 5–99 | ~100,000 combined |
| **Total** | **1,000,000** |

Baseline:

```scala
df.repartition(8, $"distribution_key")
```

Observed partitions:

```text
Partition 0 -> 113686 rows
Partition 1 -> 17893 rows
Partition 2 -> 10526 rows
Partition 3 -> 62631 rows
Partition 4 -> 63686 rows
Partition 5 -> 705263 rows
Partition 6 -> 14736 rows
Partition 7 -> 11579 rows
```

The maximum/minimum ratio was approximately **67.0x**. The largest partition contained **70.5%** of the dataset and was about **5.64x** the ideal eight-way average of 125,000 rows.

## Solution: Salting

Add a second distribution dimension to the hot key. Instead of partitioning by:

```text
distribution_key
```

partition by:

```text
(distribution_key, salt)
```

This experiment uses eight salt values:

```scala
private val NumSalts = 8
```

Salt is generated only for hot key `0`:

```scala
when(
  $"distribution_key" === 0,
  pmod($"id", lit(NumSalts))
).otherwise(lit(0))
```

The resulting partitioning key changes from `distribution_key` to `(distribution_key, salt)`.

## Experiment Configuration

Implementation:

```text
src/main/scala/com/shrikant/spark/partitioning/DataSkewMitigationExperiment.scala
```

Configuration:

```text
Rows              = 1,000,000
Output partitions = 8
Hot key           = 0
Hot-key rows      = 500,000
Number of salts   = 8
Spark master      = local[2]
Spark version     = 3.5.1
```

Baseline:

```scala
val baseline = df.repartition(NumPartitions, $"distribution_key")
```

Salted:

```scala
val salted =
  df.withColumn(
    "salt",
    when(
      $"distribution_key" === 0,
      pmod($"id", lit(NumSalts))
    ).otherwise(lit(0))
  )

val saltedPartitioned =
  salted.repartition(
    NumPartitions,
    $"distribution_key",
    $"salt"
  )
```

## Baseline Physical Plan

```text
== Physical Plan ==
AdaptiveSparkPlan (4)
+- Exchange (3)
   +- Project (2)
      +- Range (1)

(3) Exchange
Input [2]: [distribution_key#2L, id#0L]
Arguments: hashpartitioning(distribution_key#2L, 8), REPARTITION_BY_NUM
```

The important operator is:

```text
Exchange
  hashpartitioning(distribution_key, 8)
```

This is the shuffle boundary. Spark redistributes rows according to the hash of `distribution_key`. The hot key creates a major hotspot because its records are routed according to the same logical partitioning key.

## Verify the Hot-Key Salt Distribution

Observed salt distribution for key `0`:

```text
salt 0 -> 62500
salt 1 -> 62500
salt 2 -> 62500
salt 3 -> 62500
salt 4 -> 62500
salt 5 -> 62500
salt 6 -> 62500
salt 7 -> 62500
```

Total:

```text
8 × 62,500 = 500,000
```

This exactly matches the hot-key row count.

Important: the eight salts do **not** map one-to-one to Spark partitions. Spark hashes the complete `(distribution_key, salt)` key.

## Salted Physical Plan

Observed partition distribution:

```text
Partition 0 -> 63682 rows
Partition 1 -> 16842 rows
Partition 2 -> 213685 rows
Partition 3 -> 74079 rows
Partition 4 -> 113686 rows
Partition 5 -> 134474 rows
Partition 6 -> 250131 rows
Partition 7 -> 133421 rows
```

Physical plan:

```text
== Physical Plan ==
AdaptiveSparkPlan (5)
+- Exchange (4)
   +- Project (3)
      +- Project (2)
         +- Range (1)

(4) Exchange
Input [3]: [distribution_key#2L, id#0L, salt#6L]
Arguments: hashpartitioning(distribution_key#2L, salt#6L, 8), REPARTITION_BY_NUM
```

The key physical-plan change is:

```text
hashpartitioning(distribution_key, 8)
```

becoming:

```text
hashpartitioning(distribution_key, salt, 8)
```

The shuffle remains. Salting does **not** eliminate the shuffle; it changes how records are distributed within it.

## Before vs After

| Metric | Baseline | Salted |
|---|---:|---:|
| Partitions | 8 | 8 |
| Minimum rows | 10,526 | 16,842 |
| Maximum rows | 705,263 | 250,131 |
| Max / min | ~67.0x | ~14.85x |
| Largest partition share | 70.5% | 25.0% |
| Ideal average | 125,000 | 125,000 |
| Largest / ideal | ~5.64x | ~2.00x |

Maximum partition size fell from 705,263 to 250,131 rows: **455,132 fewer rows, or approximately 64.5% reduction**.

The maximum/minimum ratio improved from ~67.0x to ~14.85x, approximately a **77.8% reduction** in the ratio.

The largest partition share decreased from 70.5% to 25.0%, a reduction of approximately **45.5 percentage points**.

## Important Observation: Salting Did Not Produce Perfect Balance

This is an important result, not a failure.

The salted partitions were:

```text
63682
16842
213685
74079
113686
134474
250131
133421
```

The largest partition was still approximately 2.0x the ideal average.

Only the dominant hot key was salted. Other large keys remained unsalted:

```text
key 1 -> 200,000
key 2 -> 100,000
key 3 -> 50,000
key 4 -> 50,000
```

A production strategy may therefore need targeted treatment for multiple hot keys rather than assuming one salting operation will perfectly balance the complete dataset.

## Task and Shuffle Evidence

The salted repartitioning retained a shuffle boundary. Runtime evidence showed an upstream `ShuffleMapStage` with 2 tasks and a downstream `ResultStage` with 8 tasks.

The downstream tasks consumed local shuffle blocks in this Windows `local[2]` execution, so observed remote fetches were 0.

That does **not** mean there is no network cost. This is a single-machine local run. On a real cluster, the same `Exchange` can involve substantial network traffic between executors and nodes.

## Correctness Check

The experiment validated row-count preservation:

```text
Baseline row count = 1000000
Salted row count   = 1000000
Counts equal       = true
```

This establishes basic row-count correctness, but not semantic equivalence for every downstream operation. A salted aggregation or join must account for the additional salt dimension and eventually restore the original business-key semantics.

## Why Salting Works

Without salting:

```text
distribution_key = 0
        |
        v
hash(distribution_key)
        |
        v
one hash bucket
        |
        v
large partition
```

With salting:

```text
distribution_key = 0
        |
        +---- salt 0
        +---- salt 1
        +---- salt 2
        +---- ...
        +---- salt 7
        |
        v
hash(distribution_key, salt)
        |
        +---- multiple hash buckets
        |
        v
more parallel work
```

A single logical hot key is represented by multiple physical partitioning keys, giving Spark more opportunities to distribute its records across shuffle partitions.

## Salting for Aggregations

For a skewed aggregation such as:

```scala
df.groupBy($"customer_id")
  .agg(sum($"amount"))
```

a salted design commonly uses two phases.

First, partially aggregate by `(customer_id, salt)`. Then combine those partial results by the original `customer_id`.

Conceptually:

```text
Raw records
    |
    v
(customer_id, salt)
    |
    v
partial aggregation
    |
    v
customer_id
    |
    v
final aggregation
```

## Salting for Skewed Joins

Joins require additional care.

A common strategy is to salt the large side and replicate affected hot-key rows on the smaller side across the required salt values:

```text
Large side
    |
    | add salt
    v
(customer_id, salt)

Small side
    |
    | replicate hot keys
    v
(customer_id, salt)
    |
    v
join
```

This allows large-side hot-key records to be processed by multiple tasks, at the cost of expanding the relevant small-side rows.

## Production Trade-offs

Salting is powerful, but it is not free.

### Additional columns

The salted key introduces an additional distribution dimension.

### More complex logic

The application must determine which keys are hot, how many salt values to use, how salts are generated, how results are recombined, and whether both join sides require modification.

### Small-side replication

For skewed joins, replicating hot-key rows on the smaller side increases its size.

### Additional aggregation

Salted aggregations often require a final aggregation to combine salted partial results.

### Choosing the salt count

Too few salts leave the hot key as a bottleneck. Too many salts add partitioning and intermediate-data overhead. The salt count should be based on observed workload characteristics.

## When Salting Is a Good Choice

Consider salting when:

- one or a few keys dominate the workload
- hash partitioning creates a clear hotspot
- the workload is shuffle-heavy
- a small number of keys create long-running tasks
- increasing the overall partition count does not solve the underlying hotspot
- the application can tolerate additional aggregation or replication logic

A production diagnostic sequence is:

```text
1. Identify slow stages
2. Inspect task-duration distribution
3. Inspect partition/input-size distribution
4. Identify hot keys
5. Confirm skew is responsible
6. Choose mitigation
7. Re-run and compare task distribution
```

Do not salt first and diagnose later.

## When Salting Is Not the Right Answer

If the workload is already reasonably balanced, salting adds complexity without necessarily adding value.

Other causes of slow Spark jobs can include insufficient resources, poor partition counts, expensive UDFs, inefficient joins, serialization overhead, poor file layout, small files, GC pressure, or data-source bottlenecks.

If skew is not the actual bottleneck, salting may make the application more complicated without improving performance.

## Salting vs Increasing Partition Count

Increasing `8 partitions -> 100 partitions` does not necessarily solve a hot-key problem.

If partitioning remains:

```text
hash(distribution_key)
```

the hot key is still routed according to that same logical key. More buckets do not inherently split one hot key into multiple physical keys.

Salting changes the effective partitioning key:

```text
hash(distribution_key, salt)
```

This addresses a different problem from simply increasing the partition count.

## Salting vs AQE

Adaptive Query Execution can help with runtime optimization, including adaptive partition management and skew-aware execution in supported scenarios.

However, AQE should not be treated as proof that explicit salting is unnecessary.

In this experiment AQE was enabled, while the partitioning key was explicitly changed and the resulting distribution was measured. The experiment demonstrates a separate engineering technique: changing the data distribution itself.

It does not claim that AQE cannot mitigate skew.

## Partition Imbalance vs Data Skew

### Partition imbalance

Partitions contain different amounts of data or work.

### Data skew

The underlying values are highly non-uniform, often causing partition imbalance when data is partitioned by that key.

Module 1.5.6 demonstrated that even uniformly frequent keys can produce some hash-bucket imbalance.

Module 1.5.7 deliberately introduced highly uneven key frequencies and demonstrated severe partition imbalance.

Module 1.5.8 showed that salting the dominant hot key can materially reduce that imbalance.

The progression is:

```text
Hash partitioning
       |
       v
Key distribution
       |
       v
Partition distribution
       |
       v
Task workload
       |
       v
Skew diagnosis
       |
       v
Salting
       |
       v
Improved distribution
```

## What the Physical Plan Tells Us

The baseline plan contains:

```text
Exchange
  hashpartitioning(distribution_key, 8)
```

The salted plan contains:

```text
Exchange
  hashpartitioning(distribution_key, salt, 8)
```

This is the most important physical-plan evidence.

The application changes the logical representation of the partitioning key, and the physical plan reflects that change. The shuffle remains; the routing key changes.

This is why production Spark troubleshooting should begin with physical plans and runtime metrics rather than API names alone.

## Benchmark Limitations

This experiment runs with `local[2]` on a single Windows machine.

Therefore:

- shuffle blocks are local
- network transfer is not representative of a cluster
- executor concurrency is limited
- JVM startup and code generation can influence small timings
- local task-duration differences should not be treated as production-scale benchmarks

The strongest evidence is the physical partitioning plan, measured partition sizes, hot-key salt distribution, row-count correctness, and the architectural change in the partitioning key.

## Results Summary

```text
Baseline maximum partition:
705,263 rows

Salted maximum partition:
250,131 rows

Reduction:
64.5%
```

```text
Baseline max/min:
~67.0x

Salted max/min:
~14.85x
```

Approximate reduction in the ratio: **77.8%**.

Largest partition share:

```text
70.5%  ->  25.0%
```

Largest partition relative to ideal average:

```text
5.64x  ->  2.00x
```

Row-count correctness:

```text
1,000,000 == 1,000,000
```

The result is a successful skew-mitigation experiment while demonstrating that targeted salting does not guarantee perfect partition balance.

## Production Decision Framework

When a Spark stage shows a long-running task, ask:

```text
Is the task processing substantially more data?
        |
        +-- No --> investigate CPU, GC, I/O, serialization, UDFs
        |
        +-- Yes
             |
             v
       Is the imbalance caused by key distribution?
             |
             +-- No --> investigate partitioning/layout
             |
             +-- Yes
                  |
                  v
             Is AQE sufficient?
                  |
                  +-- Yes --> use AQE
                  |
                  +-- No
                       |
                       v
                  Consider salting
                       |
                       v
                  Validate downstream semantics
                       |
                       v
                  Benchmark again
```

This keeps salting as a targeted engineering intervention rather than a default optimization.

## Interview Takeaways

### What is data skew in Spark?

Data skew occurs when a small number of keys account for a disproportionately large amount of data or work, causing some partitions and tasks to be much heavier than others.

### Why does hash partitioning create skew?

Hash partitioning routes equal keys to the same partitioning bucket. If one key is extremely frequent, its bucket can become disproportionately large.

### What is salting?

Salting adds an additional value to the partitioning key so that a hot logical key can be represented by multiple physical keys, for example `(customer_id, salt)` instead of `customer_id`.

### Does salting remove the shuffle?

No. The experiment still shows `Exchange` and `hashpartitioning(...)`. Salting changes record distribution within the shuffle rather than eliminating the shuffle.

### Does salting guarantee perfectly balanced partitions?

No. The experiment reduced the maximum partition from 705,263 to 250,131 rows, but the salted distribution was still uneven because other large keys remained unsalted.

### Why not simply increase the number of partitions?

Because a hot key remains associated with the same logical partitioning key. Increasing the number of buckets does not inherently split that hot key into multiple physical keys.

### How would you salt a skewed join?

Typically, salt the large side and replicate affected hot-key rows on the smaller side across the required salt values, then join using the composite key.

### How would you salt an aggregation?

Perform partial aggregation using the salted key and then combine the partial results using the original business key.

## Final Engineering Lesson

> Spark performance is heavily influenced by how data is distributed, not just by how many partitions exist.

Module 1.5.8 demonstrates this directly.

The baseline used:

```text
hash(distribution_key)
```

which concentrated the dominant hot key into one partition.

The salted design changed the distribution to:

```text
hash(distribution_key, salt)
```

and reduced the dominant partition from 705,263 rows to 250,131 rows without losing rows.

The result is not a claim that salting always makes Spark fast. It is evidence for a more useful production principle:

> When a Spark workload is bottlenecked by a small number of hot keys, change the data distribution deliberately rather than blindly increasing resources or partition counts.

That is the core engineering skill this experiment is intended to demonstrate.
