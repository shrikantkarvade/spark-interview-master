Absolutely. Below is the **copy-paste-ready final Markdown for Module 1.7.6**, based on the actual console output, physical plans, and Spark UI evidence from your experiment.

# Module 1.7.6 — AQE Coalescing

## Experiment Objective

Understand how **Apache Spark Adaptive Query Execution (AQE)** dynamically coalesces shuffle partitions after observing actual runtime shuffle statistics.

This experiment compares:

1. **AQE OFF**
2. **AQE ON + shuffle partition coalescing ON**

The experiment deliberately keeps:

```text
spark.sql.shuffle.partitions = 200
```

constant in both runs.

The objective is to demonstrate that the configured shuffle partition count is **not necessarily the same as the number of partitions used for final post-shuffle execution when AQE coalescing is enabled**.

---

# 1. Why This Experiment Matters

A common Spark production configuration is:

```text
spark.sql.shuffle.partitions = 200
```

However, configuring 200 partitions does not necessarily mean Spark will perform the final computation using 200 effective partitions.

With AQE enabled, Spark can inspect the actual shuffle output and dynamically coalesce small adjacent shuffle partitions.

Conceptually:

```text
Configured Shuffle Partitions
            │
            ▼
       200 partitions
            │
            ▼
     Execute Shuffle
            │
            ▼
   Collect Runtime Statistics
            │
            ▼
     AQE Optimization
            │
            ▼
   Coalesce Small Partitions
            │
            ▼
   Fewer Effective Partitions
```

This is particularly useful when a static shuffle configuration is intentionally conservative for large production workloads but the actual workload is smaller.

---

# 2. Experiment Scenario

We simulate a transaction dataset:

```text
Transactions = 10,000,000
Customers    = 1,000,000
```

Each transaction contains:

```text
transaction_id
customer_id
amount
```

The aggregation is:

```scala
transactions
  .groupBy("customer_id")
  .sum("amount")
```

The experiment uses:

```text
Configured shuffle partitions = 200
AQE advisory partition size    = 64 MB
```

---

# 3. Experiment Matrix

| Run   | AQE | Coalescing | Shuffle Partitions | Advisory Size |
| ----- | --- | ---------- | -----------------: | ------------: |
| Run 1 | OFF | OFF        |                200 |         64 MB |
| Run 2 | ON  | ON         |                200 |         64 MB |

The only important execution difference is AQE/coalescing.

This makes the comparison easier to reason about.

---

# 4. Dataset Generation

The experiment uses deterministic data generation:

```scala
val transactions =
  spark.range(0, transactionCount)
    .selectExpr(
      "id AS transaction_id",
      s"id % $customerCount AS customer_id",
      "id % 1000 AS amount"
    )
```

For the benchmark:

```text
transactionCount = 10,000,000
customerCount   = 1,000,000
```

Therefore:

```text
Input rows = 10,000,000
```

The generated customer IDs are deterministic:

```text
customer_id = id % 1,000,000
```

This gives approximately:

```text
10 transactions per customer
```

on average.

---

# 5. Aggregation Query

The core workload is:

```scala
val aggregation =
  transactions
    .groupBy("customer_id")
    .sum("amount")
```

Spark performs partial aggregation before the shuffle.

Conceptually:

```text
10M input rows
      │
      ▼
Partial HashAggregate
      │
      ▼
2M shuffle records
      │
      ▼
Exchange
      │
      ▼
Final HashAggregate
      │
      ▼
Customer-level results
```

The presence of:

```text
partial_sum(amount)
```

in the physical plan confirms that Spark performs map-side partial aggregation.

---

# 6. Run 1 — AQE OFF

## Configuration

```text
spark.sql.adaptive.enabled = false
spark.sql.adaptive.coalescePartitions.enabled = false
spark.sql.shuffle.partitions = 200
spark.sql.adaptive.advisoryPartitionSizeInBytes = 64 MB
```

Input partitions:

```text
2
```

---

# 7. Run 1 — Initial Physical Plan

Spark produced:

```text
== Physical Plan ==
*(2) HashAggregate(keys=[customer_id#3L], functions=[sum(amount#4L)])
+- Exchange hashpartitioning(customer_id#3L, 200), ENSURE_REQUIREMENTS, [plan_id=30]
   +- *(1) HashAggregate(keys=[customer_id#3L], functions=[partial_sum(amount#4L)])
      +- *(1) Project [(id#0L % 1000000) AS customer_id#3L, (id#0L % 1000) AS amount#4L]
         +- *(1) Range (0, 10000000, step=1, splits=2)
```

The important part is:

```text
Exchange hashpartitioning(customer_id#3L, 200)
```

Therefore Spark initially creates:

```text
200 shuffle partitions
```

---

# 8. Run 1 — Execution Evidence

The scheduler reported:

```text
Got job 0 ... with 200 output partitions
Submitting 200 missing tasks from ResultStage 1
```

The final stage completed:

```text
200 / 200 tasks
```

Therefore, with AQE disabled:

```text
Configured partitions = 200
Final reducer tasks    = 200
```

There is no adaptive coalescing.

---

# 9. Run 1 — Spark UI Evidence

Spark UI reported:

```text
number of partitions: 200
```

Important metrics:

| Metric                   |        Result |
| ------------------------ | ------------: |
| Input rows               |    10,000,000 |
| Shuffle records written  |     2,000,000 |
| Shuffle bytes written    |      18.7 MiB |
| Shuffle partitions       |           200 |
| Remote bytes read        |           0 B |
| Remote blocks read       |             0 |
| Local blocks read        |           400 |
| Shuffle spill            |           0 B |
| Peak aggregation memory  | 192 MiB total |
| Maximum task peak memory |        96 MiB |
| Application action time  |       4.124 s |

Maximum aggregation build time:

```text
1.1 s
Stage 0 — Task 1
```

Maximum WholeStageCodegen duration:

```text
1.6 s
Stage 0 — Task 0
```

Maximum shuffle write time:

```text
153 ms
Stage 0 — Task 0
```

---

# 10. Run 1 — Final Physical Plan

The final physical plan remained:

```text
*(2) HashAggregate(keys=[customer_id#3L], functions=[sum(amount#4L)])
+- Exchange hashpartitioning(customer_id#3L, 200), ENSURE_REQUIREMENTS, [plan_id=30]
   +- *(1) HashAggregate(keys=[customer_id#3L], functions=[partial_sum(amount#4L)])
      +- *(1) Project [(id#0L % 1000000) AS customer_id#3L, (id#0L % 1000) AS amount#4L]
         +- *(1) Range (0, 10000000, step=1, splits=2)
```

There is no:

```text
AdaptiveSparkPlan
```

and no:

```text
AQEShuffleRead
```

Therefore:

```text
AQE = OFF
```

---

# 11. Run 2 — AQE ON + Coalescing ON

Configuration:

```text
spark.sql.adaptive.enabled = true
spark.sql.adaptive.coalescePartitions.enabled = true
spark.sql.shuffle.partitions = 200
spark.sql.adaptive.advisoryPartitionSizeInBytes = 64 MB
```

Input partitions remained:

```text
2
```

The configured shuffle partition count also remained:

```text
200
```

---

# 12. Run 2 — Initial Physical Plan

Spark initially produced:

```text
== Physical Plan ==
AdaptiveSparkPlan isFinalPlan=false
+- HashAggregate(keys=[customer_id#24L], functions=[sum(amount#25L)])
   +- Exchange hashpartitioning(customer_id#24L, 200), ENSURE_REQUIREMENTS, [plan_id=105]
      +- HashAggregate(keys=[customer_id#24L], functions=[partial_sum(amount#25L)])
         +- Project [(id#21L % 1000000) AS customer_id#24L, (id#21L % 1000) AS amount#25L]
            +- Range (0, 10000000, step=1, splits=2)
```

Notice that the initial plan still contains:

```text
Exchange hashpartitioning(customer_id#24L, 200)
```

This is important.

AQE does **not** immediately replace the configured 200 partitions.

Spark first executes the shuffle and gathers runtime statistics.

---

# 13. AQE Runtime Decision

The Spark executor logged:

```text
ShufflePartitionsUtil:
For shuffle(1),
advisory target size: 67108864,
actual target size: 10451929,
minimum partition size: 1048576
```

The configured advisory target was:

```text
64 MB
```

or:

```text
67,108,864 bytes
```

Spark calculated an actual target around:

```text
10,451,929 bytes
```

approximately:

```text
9.97 MiB
```

The actual shuffle data was therefore much smaller than the advisory target.

This provided AQE with an opportunity to coalesce small shuffle partitions.

---

# 14. AQE Coalescing Decision

The most important runtime evidence was:

```text
Got job 2 ... with 2 output partitions
Submitting 2 missing tasks from ResultStage 4
```

Therefore the final adaptive execution used:

```text
2 reducer tasks
```

instead of:

```text
200 reducer tasks
```

---

# 15. Run 2 — Final Adaptive Physical Plan

Spark UI provided the decisive evidence:

```text
== Physical Plan ==
AdaptiveSparkPlan (12)
+- == Final Plan ==
   DeserializeToObject (8)
   +- * HashAggregate (7)
      +- AQEShuffleRead (6)
         +- ShuffleQueryStage (5), Statistics(sizeInBytes=45.8 MiB, rowCount=2.00E+6)
            +- Exchange (4)
               +- * HashAggregate (3)
                  +- * Project (2)
                     +- * Range (1)
+- == Initial Plan ==
   DeserializeToObject (11)
   +- HashAggregate (10)
      +- Exchange (9)
         +- HashAggregate (3)
            +- Project (2)
               +- Range (1)
```

The critical node is:

```text
AQEShuffleRead
```

with:

```text
Arguments: coalesced
```

Spark UI additionally reported:

```text
number of partitions: 2
number of coalesced partitions: 2
```

This is direct proof that AQE coalescing occurred.

---

# 16. The Most Important Observation

Notice the difference:

### Initial Exchange

```text
Exchange
hashpartitioning(customer_id, 200)
```

### Final Adaptive Read

```text
AQEShuffleRead
Arguments: coalesced
```

Therefore:

```text
Initial configured shuffle = 200
                    ↓
             Runtime statistics
                    ↓
            AQE coalescing
                    ↓
Effective final partitions = 2
```

This demonstrates the distinction between:

```text
Configured shuffle partitions
```

and:

```text
Effective runtime execution partitions
```

---

# 17. Run 2 — Spark UI Metrics

Important metrics:

| Metric                        |        Result |
| ----------------------------- | ------------: |
| Input rows                    |    10,000,000 |
| Configured shuffle partitions |           200 |
| Coalesced partitions          |         **2** |
| Shuffle records written       |     2,000,000 |
| Shuffle bytes written         |      18.7 MiB |
| AQE shuffle read              | **Coalesced** |
| Remote bytes read             |           0 B |
| Local bytes read              |      18.7 MiB |
| Local blocks read             |             4 |
| Shuffle spill                 |           0 B |
| Peak aggregation memory       | 192 MiB total |
| Maximum task peak memory      |        96 MiB |
| Application action time       |   **1.450 s** |

Maximum WholeStageCodegen duration:

```text
1.1 s
Stage 2 — Task 202
```

Maximum aggregation build time:

```text
862 ms
Stage 2 — Task 202
```

Maximum shuffle write time:

```text
110 ms
Stage 2 — Task 203
```

---

# 18. Complete Benchmark Comparison

| Metric                        |     AQE OFF | AQE ON + Coalescing |
| ----------------------------- | ----------: | ------------------: |
| AQE                           |         OFF |                  ON |
| Coalescing                    |         OFF |                  ON |
| Configured shuffle partitions |         200 |                 200 |
| Initial Exchange              |         200 |                 200 |
| Effective final partitions    |     **200** |               **2** |
| Final reducer tasks           |     **200** |               **2** |
| Input rows                    |         10M |                 10M |
| Shuffle records               |          2M |                  2M |
| Shuffle bytes                 |    18.7 MiB |            18.7 MiB |
| Remote bytes                  |         0 B |                 0 B |
| Spill                         |         0 B |                 0 B |
| AQEShuffleRead                |          No |             **Yes** |
| Coalesced partitions          |           — |               **2** |
| Application action time       | **4.124 s** |         **1.450 s** |

---

# 19. Performance Observation

Measured application action time:

```text
AQE OFF = 4.124 s
AQE ON  = 1.450 s
```

Observed reduction:

```text
(4.124 - 1.450) / 4.124 × 100
≈ 64.8%
```

Therefore:

```text
Observed local benchmark improvement ≈ 64.8%
```

However, this number should **not** be interpreted as a universal production improvement.

This experiment was executed:

* on a local Windows environment
* with only 2 input partitions
* with a relatively small shuffle volume
* without remote network shuffle
* without concurrent production workloads

The important production lesson is therefore not:

> "AQE always improves Spark performance by 65%."

The correct engineering conclusion is:

> **AQE coalescing can substantially reduce unnecessary post-shuffle task overhead when the actual shuffle data is much smaller than the configured partitioning assumes.**

---

# 20. Why Did Shuffle Bytes Stay the Same?

A common misconception is:

> "If AQE reduces 200 partitions to 2, Spark must reduce the shuffle data."

Not necessarily.

In this experiment:

```text
Shuffle bytes written
AQE OFF = 18.7 MiB
AQE ON  = 18.7 MiB
```

The shuffle itself still produces the same logical data.

AQE coalescing primarily changes how that already-produced shuffle data is **read and processed after the shuffle**.

Conceptually:

```text
Before AQE:

200 shuffle partitions
        ↓
200 reducer tasks


With AQE:

200 shuffle partitions
        ↓
AQE analyzes shuffle statistics
        ↓
Coalesces compatible small partitions
        ↓
2 effective post-shuffle partitions
        ↓
2 reducer tasks
```

Therefore:

```text
Shuffle data volume ≠ number of final execution tasks
```

---

# 21. Why Was Partial Aggregation Important?

The physical plan contains:

```text
HashAggregate
Functions:
partial_sum(amount)
```

before the Exchange.

Therefore Spark performs partial aggregation before shuffling.

The benchmark generated:

```text
10,000,000 input rows
```

but only:

```text
2,000,000 shuffle records
```

were written.

This demonstrates another important Spark optimization:

```text
Input
10M rows
   ↓
Partial HashAggregate
   ↓
2M shuffle records
   ↓
Exchange
```

The combination of:

```text
Partial Aggregation
+
AQE Coalescing
```

makes the execution considerably more efficient for this workload.

---

# 22. Understanding `spark.sql.shuffle.partitions`

The experiment demonstrates that:

```text
spark.sql.shuffle.partitions = 200
```

should not automatically be interpreted as:

```text
"Spark will always execute 200 final reducer tasks."
```

Instead:

```text
spark.sql.shuffle.partitions
```

provides the initial shuffle partitioning.

With AQE enabled:

```text
Initial partitioning
        ↓
Runtime statistics
        ↓
Adaptive optimization
        ↓
Potentially different effective execution
```

This distinction is extremely important in production Spark tuning.

---

# 23. AQE Coalescing vs Increasing Shuffle Partitions

Consider a production workload where:

```text
spark.sql.shuffle.partitions = 2000
```

is intentionally configured because some large workloads produce substantial shuffle data.

A smaller workload may not need all 2000 reducer tasks.

Without AQE:

```text
Small workload
     ↓
2000 partitions
     ↓
Potential task overhead
```

With AQE:

```text
Small workload
     ↓
2000 initial partitions
     ↓
Runtime statistics
     ↓
AQE coalescing
     ↓
Fewer effective partitions
```

This allows a more conservative global configuration while allowing Spark to adapt to smaller workloads.

---

# 24. AQE Coalescing Does Not Solve Every Partition Problem

AQE coalescing is particularly useful for:

```text
Too many small post-shuffle partitions
```

It does not automatically solve:

```text
Large data skew
```

or:

```text
A few extremely large partitions
```

These are different problems.

Conceptually:

```text
Problem A: Too many small partitions
        ↓
AQE Coalescing
```

versus:

```text
Problem B: One/few huge skewed partitions
        ↓
AQE Skew Join / skew handling
```

Therefore, partition optimization must be driven by actual Spark UI evidence rather than by blindly increasing or decreasing:

```text
spark.sql.shuffle.partitions
```

---

# 25. Spark UI Investigation Checklist

When investigating AQE coalescing in production, inspect the Spark UI.

## SQL tab

Look for:

```text
AdaptiveSparkPlan
```

and:

```text
AQEShuffleRead
```

Check whether the final plan contains:

```text
Arguments: coalesced
```

---

## Stage tab

Compare:

```text
Initial shuffle partitions
```

with:

```text
Final reducer task count
```

Look for:

* number of tasks
* task duration distribution
* shuffle read
* shuffle write
* input size
* output size
* spill
* executor CPU time

---

## SQL Metrics

Useful metrics include:

```text
number of partitions
number of coalesced partitions
partition data size
shuffle bytes written
shuffle records written
local bytes read
remote bytes read
spill size
aggregation build time
peak memory
```

---

# 26. How to Read This Experiment Like a Production Engineer

Instead of asking:

> "How many shuffle partitions did I configure?"

Ask:

> "How many partitions did Spark actually execute after adaptive optimization?"

Then investigate:

```text
Configured partitions
        ↓
Actual shuffle size
        ↓
AQE decision
        ↓
Coalesced partitions
        ↓
Final task count
        ↓
Task distribution
        ↓
Runtime
```

This is much more useful than tuning a single configuration value in isolation.

---

# 27. Engineering Lessons

## Lesson 1 — Configuration Is Not Always Execution

```text
spark.sql.shuffle.partitions = 200
```

does not necessarily mean:

```text
200 final reducer tasks
```

with AQE enabled.

---

## Lesson 2 — AQE Uses Runtime Information

Spark cannot know the exact shuffle distribution before executing the shuffle.

AQE therefore follows:

```text
Plan
 ↓
Execute
 ↓
Collect statistics
 ↓
Re-optimize
 ↓
Execute adapted plan
```

---

## Lesson 3 — Coalescing Reduces Task Overhead

In this experiment:

```text
200 → 2
```

effective post-shuffle partitions.

This can reduce:

* scheduler overhead
* task startup overhead
* task bookkeeping
* excessive tiny-task execution

---

## Lesson 4 — Coalescing Does Not Mean Less Shuffle Data

The experiment produced:

```text
18.7 MiB
```

of shuffle data in both runs.

The major difference was how the shuffle was consumed.

---

## Lesson 5 — Spark UI Is Critical

The console output showed evidence of AQE:

```text
Got job 2 ... with 2 output partitions
```

but the Spark UI provided definitive proof:

```text
AQEShuffleRead
Arguments: coalesced

number of coalesced partitions: 2
```

For production investigations, always validate the runtime behavior through Spark UI metrics and the final adaptive plan.

---

# 28. Production Tuning Guidance

A practical production approach is:

### Step 1 — Start with a reasonable shuffle partition configuration

Avoid blindly setting extremely high or extremely low values.

---

### Step 2 — Enable AQE where appropriate

AQE allows Spark to make runtime decisions based on actual data.

---

### Step 3 — Inspect actual shuffle sizes

Look at:

```text
Shuffle Read
Shuffle Write
Partition Size
Task Distribution
```

---

### Step 4 — Check for small-partition problems

If many partitions contain very little data, AQE coalescing may help.

---

### Step 5 — Check for skew separately

If a few partitions are dramatically larger than others, investigate skew rather than simply increasing the partition count.

---

### Step 6 — Validate with representative workloads

Never conclude that a configuration is optimal from a single local benchmark.

Test:

```text
Small workload
Medium workload
Large workload
Skewed workload
Production-like workload
```

---

# 29. Important Difference: Coalescing vs Repartitioning

AQE coalescing should not be confused with:

```scala
df.repartition(...)
```

or:

```scala
df.coalesce(...)
```

Application-level partition operations are explicit transformations.

AQE coalescing is an **optimizer/runtime decision** made by Spark after shuffle statistics become available.

Conceptually:

```text
Application code
      │
      ▼
Logical / physical plan
      │
      ▼
Spark executes shuffle
      │
      ▼
AQE observes runtime statistics
      │
      ▼
AQE modifies post-shuffle execution
```

This is one of the key ideas behind Adaptive Query Execution.

---

# 30. Interview Questions

## Q1. What is AQE?

Adaptive Query Execution is a Spark SQL optimization mechanism that uses runtime statistics collected during execution to modify parts of the physical execution plan.

---

## Q2. What does AQE coalescing do?

It combines small post-shuffle partitions into fewer larger partitions to reduce unnecessary task overhead.

---

## Q3. Does AQE change `spark.sql.shuffle.partitions`?

It does not necessarily change the configured value itself.

Instead, AQE can change the effective post-shuffle partition processing through adaptive mechanisms such as `AQEShuffleRead`.

---

## Q4. Why can Spark start with 200 partitions but finish with 2?

Because Spark first creates the configured shuffle partitioning, observes the actual shuffle data size, and AQE then coalesces small partitions when appropriate.

---

## Q5. How did this experiment prove coalescing?

The Spark UI final plan contained:

```text
AQEShuffleRead
Arguments: coalesced
```

and reported:

```text
number of coalesced partitions: 2
```

while the Exchange remained configured for:

```text
200
```

---

## Q6. Does AQE coalescing reduce shuffle write volume?

Not necessarily.

In this experiment:

```text
Shuffle bytes:
18.7 MiB → 18.7 MiB
```

The optimization reduced the effective post-shuffle execution partitions rather than reducing the amount of data initially written by the shuffle.

---

## Q7. When can AQE coalescing be useful?

It is particularly useful when:

* many shuffle partitions are very small
* the configured shuffle partition count is conservative
* workloads vary significantly in size
* small-task scheduling overhead becomes significant

---

## Q8. Does AQE coalescing solve data skew?

No.

Coalescing addresses excessive small partitions.

Skew requires separate investigation and potentially AQE skew handling or application-level techniques.

---

# 31. Key Takeaways

The experiment demonstrated:

```text
Configured shuffle partitions = 200
```

but:

```text
AQE OFF
    ↓
200 effective reducer tasks
```

while:

```text
AQE ON
    ↓
Runtime shuffle statistics
    ↓
AQE coalescing
    ↓
2 effective reducer tasks
```

The final adaptive plan explicitly contained:

```text
AQEShuffleRead
Arguments: coalesced
```

and Spark UI confirmed:

```text
number of coalesced partitions: 2
```

The measured local benchmark was:

```text
AQE OFF = 4.124 s
AQE ON  = 1.450 s
```

for an observed:

```text
~64.8% reduction in application action time
```

The most important engineering lesson is:

> **Do not tune Spark only from configuration values. Tune from actual runtime behavior.**

For AQE workloads, the important question is not simply:

```text
How many shuffle partitions did I configure?
```

It is:

```text
How many partitions did Spark actually execute,
why did AQE choose that number,
and what did the Spark UI show?
```

---

# 32. Final Experiment Summary

```text
                    AQE OFF
                       │
                       ▼
                200 partitions
                       │
                       ▼
                200 reducer tasks
                       │
                       ▼
                    4.124 s


                    AQE ON
                       │
                       ▼
                200 initial partitions
                       │
                       ▼
             Runtime shuffle statistics
                       │
                       ▼
                AQE coalescing
                       │
                       ▼
                 2 partitions
                       │
                       ▼
                  2 reducer tasks
                       │
                       ▼
                    1.450 s
```

### Final Result

```text
200 → 2 effective partitions
200 → 2 reducer tasks
18.7 MiB shuffle data in both runs
0 B spill
AQEShuffleRead = coalesced
Observed action-time reduction ≈ 64.8%
```

This experiment establishes the foundation for the next aggregation experiments, particularly the interaction between **partition sizing, aggregation cardinality, ordering, Top-N processing, data skew, and AQE optimization**.
