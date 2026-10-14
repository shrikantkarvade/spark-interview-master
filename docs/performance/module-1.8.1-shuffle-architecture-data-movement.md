# Module 1.8.1 — Shuffle Architecture & Data Movement

## Overview

Shuffle is one of the most important execution mechanisms in Apache Spark.

Whenever Spark needs to redistribute records across partitions so that related records can be processed together, the physical plan introduces an `Exchange` operator.

This experiment investigates shuffle from an execution-engineering perspective using a controlled aggregation workload.

The objective is not to optimize the workload yet.

The objective is to understand:

* Why Spark creates a shuffle.
* Where the shuffle boundary appears in the physical plan.
* The relationship between input partitions and shuffle partitions.
* How partial aggregation affects shuffle volume.
* How shuffle write and read metrics appear in Spark UI.
* How Adaptive Query Execution (AQE) changes shuffle execution at runtime.
* How Jobs, Stages, Tasks, and Exchange operators relate to one another.
* Which observations are specific to local execution and which are relevant to production clusters.

---

# 1. Experiment Objective

The experiment uses a deterministic dataset and performs:

```text
GROUP BY customerId
```

The workload is intentionally simple so that the shuffle boundary can be isolated and analyzed.

The conceptual execution flow is:

```text
Input Dataset
      |
      v
Local HashAggregate
      |
      v
Exchange
hashpartitioning(customerId)
      |
      v
Shuffle
      |
      v
Final HashAggregate
      |
      v
Customer-level result
```

The experiment also examines how AQE can modify the runtime execution after Spark has collected actual shuffle statistics.

---

# 2. Benchmark Configuration

| Parameter                     |                   Value |
| ----------------------------- | ----------------------: |
| Input rows                    |              10,000,000 |
| Distinct customers            |               1,000,000 |
| Input partitions              |                       6 |
| Configured shuffle partitions |                      64 |
| AQE                           |                 Enabled |
| Execution mode                |                   Local |
| Spark UI                      | `http://localhost:4040` |

The benchmark generates customer IDs using:

```scala
id % customerCount
```

With:

```text
10,000,000 rows
1,000,000 customers
6 input partitions
```

each input partition contains the complete range of customer IDs in this deterministic workload.

This characteristic is important when interpreting the partial-aggregation output.

---

# 3. Experiment Workflow

The application performs two relevant actions.

### Query 0

The generated dataset is materialized using:

```scala
input.count()
```

This is setup/materialization work.

### Query 1

The actual shuffle experiment executes:

```scala
input
  .groupBy("customerId")
  .count()
```

The two queries must be analyzed separately because they have different physical execution characteristics.

---

# 4. Query 0 — Dataset Materialization

## 4.1 Query Information

```text
Submitted Time: 2026/09/16 22:42:55
Duration: 0.9 s
Succeeded Jobs: 0, 1
```

The purpose of this query is to materialize the input dataset before running the actual experiment.

---

## 4.2 Physical Plan

The relevant physical plan was:

```text
AdaptiveSparkPlan
+- HashAggregate
   +- ShuffleQueryStage
      +- Exchange
         +- HashAggregate
            +- Project
               +- Range
```

The important operators are:

```text
HashAggregate
      |
      v
Exchange
SinglePartition
      |
      v
HashAggregate
```

The `count()` operation uses local partial aggregation first.

The six input tasks each calculate a partial count:

```text
10,000,000 input rows
        |
        v
6 input partitions
        |
        v
6 partial counts
```

Those six partial results are then sent through:

```text
Exchange
SinglePartition
```

The final aggregation produces one result.

---

## 4.3 Query 0 Shuffle Evidence

Spark UI reported:

```text
shuffle records written: 6
shuffle bytes written: 354 B
number of output rows: 6
```

This is an extremely small shuffle.

The important execution pattern is:

```text
10M input rows
      |
      v
6 local partial counts
      |
      v
354 B shuffle
      |
      v
SinglePartition
      |
      v
1 final count
```

Spark therefore does not shuffle the original 10 million records.

It shuffles only the six partial aggregation results.

---

## 4.4 Query 0 Maximum Metrics

| Metric                         | Maximum |   Stage |   Task |
| ------------------------------ | ------: | ------: | -----: |
| WholeStageCodegen (1) duration |   77 ms | Stage 0 | Task 1 |
| Shuffle write time             |   31 ms | Stage 0 | Task 0 |
| Shuffle bytes written          |    59 B | Stage 0 | Task 1 |
| Data size                      |    16 B | Stage 0 | Task 1 |
| HashAggregate build time       |   59 ms | Stage 0 | Task 1 |

The maximum values are not evidence of a production bottleneck.

The absolute values are very small and the workload is running locally.

---

# 5. Query 1 — Actual Shuffle Experiment

## 5.1 Query Information

```text
Submitted Time: 2026/09/16 22:42:56
Duration: 3 s
Succeeded Jobs: 2, 3, 4
```

The query executed:

```scala
groupBy("customerId").count()
```

This is the primary workload for Module 1.8.1.

---

# 6. Initial Physical Plan

The initial physical plan contains:

```text
HashAggregate
   |
   v
Exchange
hashpartitioning(customerId, 64)
   |
   v
HashAggregate
   |
   v
Project
   |
   v
Range
```

The relevant section is:

```text
HashAggregate
    |
    v
Exchange
hashpartitioning(customerId, 64)
    |
    v
HashAggregate
```

This `Exchange` is the primary shuffle boundary investigated by this experiment.

---

# 7. Why Does Spark Need the Shuffle?

The input records are initially distributed across six input partitions.

Records for the same customer may exist in multiple input partitions.

For example:

```text
Input Partition 0
    customer 100
    customer 200

Input Partition 1
    customer 100
    customer 300
```

To calculate a complete customer-level aggregation, Spark must bring records belonging to the same customer into the same downstream partition.

Spark therefore applies:

```text
hashpartitioning(customerId, 64)
```

The conceptual process is:

```text
6 input partitions
       |
       v
Local aggregation
       |
       v
Exchange
hash(customerId)
       |
       +----> Shuffle partition 0
       +----> Shuffle partition 1
       +----> ...
       +----> Shuffle partition 63
```

The shuffle is therefore a consequence of the **data-distribution requirement of the aggregation**.

---

# 8. Input Partitions vs Shuffle Partitions

One of the most important lessons from this experiment is that:

```text
Input partitions
```

and:

```text
Shuffle partitions
```

are different concepts.

The experiment starts with:

```text
6 input partitions
```

but the Exchange is configured for:

```text
64 shuffle partitions
```

Conceptually:

```text
INPUT

P0
P1
P2
P3
P4
P5

        |
        v

      EXCHANGE
 hash(customerId)

        |
        v

S0  S1  S2  ...  S63
```

Input partitions describe how the source data is initially divided.

Shuffle partitions describe how Spark redistributes data after the Exchange.

---

# 9. Partial Aggregation Reduced Shuffle Records

The input contained:

```text
10,000,000 rows
```

However, the first aggregation reported:

```text
number of output rows: 6,000,000
```

and:

```text
shuffle records written: 6,000,000
```

This happens because Spark performs local aggregation before the shuffle.

For this deterministic benchmark, each of the six input partitions contains the complete set of customer IDs.

Therefore:

```text
6 input partitions
×
1,000,000 customer IDs
=
6,000,000 local aggregate records
```

The important qualification is that this relationship is specific to the benchmark's data distribution.

It is not a universal rule that:

```text
input partitions × distinct keys
```

always equals the number of shuffle records.

The general principle is:

> Partial aggregation can reduce the amount of data that must cross the shuffle boundary.

---

# 10. Shuffle Volume

Spark UI reported:

```text
shuffle records written: 6,000,000
```

and:

```text
shuffle bytes written: 29.2 MiB
```

The six input tasks produced approximately:

```text
4.9 MiB
```

of shuffle output each.

The resulting execution flow is:

```text
10M input rows
      |
      v
Local HashAggregate
      |
      v
6M partial records
      |
      v
29.2 MiB shuffle
      |
      v
Final aggregation
```

This demonstrates why partial aggregation is an important Spark execution optimization.

---

# 11. AQE Changed the Runtime Execution

The initial plan specified:

```text
hashpartitioning(customerId, 64)
```

Therefore Spark initially planned for:

```text
64 shuffle partitions
```

However, the executed plan contains:

```text
AQEShuffleRead
Arguments: coalesced
```

Spark UI reported:

```text
number of partitions: 64
number of coalesced partitions: 2
```

The total partition data size was approximately:

```text
30.9 MiB
```

with approximately:

```text
15.4 MiB per coalesced partition
```

Therefore the runtime execution effectively became:

```text
Configured:
64 shuffle partitions

          |
          v
AQE analyzes actual shuffle statistics

          |
          v

Runtime:
2 coalesced partitions
```

This is a concrete demonstration that:

> `spark.sql.shuffle.partitions` is not necessarily the final number of downstream tasks when AQE is enabled.

---

# 12. Initial Plan vs Final Plan

## Initial Plan

```text
HashAggregate
    |
Exchange
hashpartitioning(customerId, 64)
    |
HashAggregate
```

## Final Adaptive Plan

```text
HashAggregate
    |
AQEShuffleRead
coalesced
    |
ShuffleQueryStage
    |
Exchange
hashpartitioning(customerId, 64)
    |
HashAggregate
```

The Exchange still exists because the data redistribution is still required.

AQE does not eliminate the business-required shuffle.

Instead, AQE changes how the resulting shuffle data is consumed.

This distinction is important.

---

# 13. Shuffle Read Behavior

The Spark UI reported:

```text
records read: 6,000,000
local bytes read: 29.2 MiB
remote bytes read: 0 B
remote blocks read: 0
fetch wait time: 0 ms
```

The shuffle data was therefore read locally in this experiment.

This is expected because the experiment was executed in a local environment.

It should not be interpreted as evidence that production Spark applications do not incur network shuffle costs.

In a distributed cluster, shuffle data can be transferred between executors over the network.

---

# 14. Shuffle Partition Distribution

Spark UI reported:

```text
number of partitions: 64
```

before AQE coalescing.

After AQE:

```text
number of coalesced partitions: 2
```

The final partition sizes were approximately:

```text
15.4 MiB
15.4 MiB
```

This indicates a balanced runtime distribution for this particular workload.

There is no evidence of a highly uneven partition or a significant straggler caused by skew.

---

# 15. Memory and Spill Behavior

The experiment showed:

```text
spill size: 0 B
```

and:

```text
number of sort fallback tasks: 0
```

The aggregation peak memory reported approximately:

```text
96 MiB per task
```

for the relevant aggregation stage.

The later aggregation reported approximately:

```text
80 MiB per task
```

There was no meaningful evidence of:

* Disk spill.
* Sort fallback.
* Significant GC pressure.
* Memory exhaustion.

Therefore this workload does not demonstrate a memory or spill bottleneck.

This is a useful result because controlled performance experiments should identify both bottlenecks and the absence of bottlenecks.

---

# 16. Task-Level Evidence

## Primary shuffle-producing stage

The primary shuffle-producing stage was:

```text
Stage 3
```

The maximum values for several shuffle and aggregation metrics occurred on:

```text
Stage 3 → Task 7
```

Relevant maximum metrics included:

| Metric                  |  Maximum |   Stage |   Task |
| ----------------------- | -------: | ------: | -----: |
| Shuffle bytes written   |  4.9 MiB | Stage 3 | Task 7 |
| Data size               | 15.3 MiB | Stage 3 | Task 7 |
| Aggregation build time  |   538 ms | Stage 3 | Task 7 |
| Peak aggregation memory |   96 MiB | Stage 3 | Task 7 |
| WholeStageCodegen (1)   |   885 ms | Stage 3 | Task 7 |

These values identify the task associated with the maximum reported metric, but the magnitudes do not indicate problematic skew.

---

## Downstream aggregation stage

The downstream aggregation reported:

```text
Stage 5
```

with:

```text
Task 13
```

having the maximum reported values for several metrics.

| Metric                         | Maximum |   Stage |    Task |
| ------------------------------ | ------: | ------: | ------: |
| WholeStageCodegen (2) duration |   1.0 s | Stage 5 | Task 13 |
| Aggregation build time         |  999 ms | Stage 5 | Task 13 |
| Peak aggregation memory        |  80 MiB | Stage 5 | Task 13 |

Again, these values should be interpreted in the context of the local benchmark.

---

# 17. Query 1 Maximum Metric Mapping

For quick reference:

| Metric                         |      Max | Stage |   Task |
| ------------------------------ | -------: | ----: | -----: |
| WholeStageCodegen (2) duration |    1.0 s | **5** | **13** |
| WholeStageCodegen (1) duration |   885 ms | **3** |  **7** |
| Shuffle write time             |     1 ms | **5** | **14** |
| Shuffle bytes written          |  4.9 MiB | **3** |  **7** |
| Data size                      | 15.3 MiB | **3** |  **7** |
| Aggregation build time         |   538 ms | **3** |  **7** |
| Aggregation peak memory        |   96 MiB | **3** |  **7** |
| Final aggregation build time   |   999 ms | **5** | **13** |
| Final aggregation peak memory  |   80 MiB | **5** | **13** |

The distinction between the metric's maximum value and the stage that produced the primary shuffle is important.

---

# 18. Why Query 0 and Query 1 Must Be Separated

The application produced two different execution patterns.

## Query 0

```text
input.count()
```

Execution:

```text
10M rows
    |
    v
6 local partial counts
    |
    v
SinglePartition Exchange
    |
    v
1 final result
```

Shuffle:

```text
6 records
354 B
```

## Query 1

```text
groupBy(customerId).count()
```

Execution:

```text
10M rows
    |
    v
6 input partitions
    |
    v
Local HashAggregate
    |
    v
6M partial records
    |
    v
Exchange
hashpartitioning(customerId, 64)
    |
    v
AQE coalescing
64 → 2
    |
    v
Final HashAggregate
    |
    v
1M customer results
```

Shuffle:

```text
6,000,000 records
29.2 MiB
```

Therefore Query 1 is the primary evidence for Module 1.8.1.

Query 0 is useful as materialization evidence and demonstrates that Spark can also shuffle tiny partial aggregate results for a global count.

---

# 19. Complete Execution Model

The actual Query 1 execution can be represented as:

```text
                    10,000,000 rows
                           |
                           v
                  Range: 6 partitions
                           |
                           v
                    Project customerId
                           |
                           v
                  Local HashAggregate
                           |
                           v
                  6,000,000 records
                           |
                           v
                 +-------------------+
                 |      EXCHANGE     |
                 | hash(customerId)  |
                 |   64 partitions   |
                 +-------------------+
                           |
                           v
                   ShuffleQueryStage
                           |
                           v
                 AQE analyzes shuffle
                     statistics
                           |
                           v
                  64 → 2 coalesced
                     partitions
                           |
                           v
                   HashAggregate
                           |
                           v
                  1,000,000 results
```

This is the central execution story of Module 1.8.1.

---

# 20. Engineering Lessons

## 20.1 Shuffle is not inherently bad

An Exchange is not automatically a performance problem.

In this experiment, the shuffle is required because Spark must redistribute records by:

```text
customerId
```

so that the customer-level aggregation can be completed correctly.

The correct engineering question is not:

> "Can I eliminate every shuffle?"

It is:

> "Why is this shuffle required, how much data does it move, and is its cost justified by the query semantics?"

---

## 20.2 Partial aggregation can reduce shuffle volume

The workload starts with:

```text
10M input rows
```

but writes:

```text
6M shuffle records
```

because local aggregation occurs before the Exchange.

This demonstrates the importance of understanding the complete execution pipeline rather than assuming every input record is necessarily transferred across the shuffle boundary.

---

## 20.3 Input partition count and shuffle partition count are different

The experiment has:

```text
6 input partitions
```

and:

```text
64 configured shuffle partitions
```

These numbers describe different phases of execution.

Changing one does not automatically mean the other changes.

---

## 20.4 Configured shuffle partitions are not necessarily runtime task count

The application configured:

```text
spark.sql.shuffle.partitions = 64
```

but AQE coalesced the resulting shuffle partitions to:

```text
2
```

Therefore production troubleshooting must examine:

* Configuration.
* Initial physical plan.
* Executed adaptive plan.
* Spark UI.

Looking at only the configuration can produce an incomplete diagnosis.

---

## 20.5 AQE adapts after observing actual data

The initial plan could not know the exact runtime shuffle sizes.

After the shuffle was materialized, AQE had actual statistics available and coalesced the partitions.

This is one of the fundamental ideas behind Adaptive Query Execution.

---

## 20.6 Local execution hides network costs

The experiment reported:

```text
remote bytes read = 0 B
```

because the workload was executed locally.

This does not mean shuffle is network-free in production.

A multi-executor Spark application can incur significant network traffic during shuffle.

Therefore a production performance investigation should distinguish:

```text
local shuffle
```

from:

```text
distributed shuffle
```

---

## 20.7 Spark UI evidence is essential

The physical plan tells us:

```text
Exchange
hashpartitioning(customerId, 64)
```

The executed plan tells us:

```text
AQEShuffleRead
coalesced
```

The Spark UI tells us:

```text
6M shuffle records
29.2 MiB shuffle
64 initial partitions
2 coalesced partitions
```

These three sources provide different levels of evidence.

Serious Spark troubleshooting should use all three.

---

# 21. Production Troubleshooting Checklist

When a production Spark workload appears to have a shuffle problem, investigate in this order:

```text
1. Identify the Exchange
       ↓
2. Determine why the shuffle is required
       ↓
3. Measure shuffle records
       ↓
4. Measure shuffle bytes
       ↓
5. Inspect partition-size distribution
       ↓
6. Check task duration distribution
       ↓
7. Check spill
       ↓
8. Check GC
       ↓
9. Check skew / stragglers
       ↓
10. Inspect AQE behavior
       ↓
11. Compare configured vs executed partitions
       ↓
12. Determine whether optimization is actually required
```

The key principle is:

> Optimize based on observed execution behavior, not merely based on the presence of an Exchange.

---

# 22. Interview Questions

### Q1. What causes an Exchange in Spark?

An Exchange is introduced when Spark needs to redistribute data between partitions to satisfy a downstream partitioning requirement.

Examples include:

* `groupBy`
* `join`
* `orderBy`
* `distinct`
* repartitioning operations

---

### Q2. Is every Exchange a shuffle?

For the purposes of physical execution reasoning, an Exchange generally represents a data redistribution boundary and commonly corresponds to shuffle behavior.

However, the exact runtime behavior depends on the physical operator and execution plan.

The physical plan and Spark UI should be inspected rather than relying on operator names alone.

---

### Q3. Why does `groupBy(customerId)` require a shuffle?

Because records for the same customer may initially reside in different partitions.

Spark must redistribute them so that the downstream aggregation can process each customer's records together.

---

### Q4. Why did 10 million input rows result in only 6 million shuffle records?

Because local partial aggregation occurs before the shuffle.

For this deterministic benchmark, each of the six input partitions contains the complete customer-key range, resulting in:

```text
6 × 1M = 6M
```

local aggregate records.

---

### Q5. Does `spark.sql.shuffle.partitions = 64` guarantee 64 final tasks?

No.

With AQE enabled, Spark can coalesce shuffle partitions based on actual runtime statistics.

This experiment configured:

```text
64
```

but AQE produced:

```text
2 coalesced partitions
```

---

### Q6. Why did the experiment report zero remote shuffle bytes?

Because the application was executed locally.

In a distributed Spark cluster, shuffle data can be transferred between executors over the network.

---

### Q7. How would you identify a problematic shuffle in production?

Inspect:

* Shuffle read/write volume.
* Partition-size distribution.
* Task duration distribution.
* Spill.
* GC.
* Skew.
* Straggler tasks.
* AQE behavior.
* Network transfer.
* Physical-plan Exchange operators.

---

# 23. Key Takeaways

The most important observations from Module 1.8.1 are:

```text
10M input rows
       |
       v
6 input partitions
       |
       v
Local aggregation
       |
       v
6M partial records
       |
       v
Exchange
hashpartitioning(customerId, 64)
       |
       v
29.2 MiB shuffle
       |
       v
AQE
64 → 2 coalesced partitions
       |
       v
Final aggregation
       |
       v
1M customer results
```

The experiment demonstrates five core principles:

1. **Shuffle is caused by data-distribution requirements.**
2. **Partial aggregation can reduce shuffle volume.**
3. **Input partitions and shuffle partitions are different concepts.**
4. **AQE can change runtime partitioning after shuffle statistics are available.**
5. **Physical-plan analysis must be validated against Spark UI runtime evidence.**

The broader production-engineering lesson is:

> **Do not optimize Spark because you see an Exchange. Understand why the Exchange exists, measure what it moves, determine whether the resulting tasks are balanced, and then decide whether the shuffle is actually a problem.**

---

# 24. Experiment Status

**Module 1.8.1 — Shuffle Architecture & Data Movement: COMPLETE ✅**

Evidence collected:

* Console execution results.
* Initial physical plan.
* Final adaptive physical plan.
* Query 0 Spark UI metrics.
* Query 1 Spark UI metrics.
* Shuffle write metrics.
* Shuffle read metrics.
* Task-level maximum metrics.
* Aggregation memory metrics.
* Spill metrics.
* AQE partition coalescing evidence.

### Next Experiment

**Module 1.8.2 — Shuffle Partition Sizing**

The next experiment will keep the workload controlled while varying:

```text
spark.sql.shuffle.partitions
```

The experiment will investigate:

```text
8
16
32
64
128
256
```

and compare:

* Shuffle bytes.
* Shuffle records.
* Task count.
* Task duration.
* AQE coalescing.
* Partition size.
* Scheduler overhead.
* Aggregation time.
* Spill.
* GC.
* Overall execution time.

The goal will be to move from understanding **how shuffle works** to understanding **how shuffle partition sizing affects performance**.
