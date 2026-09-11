Absolutely. Here is the **next repository-ready Markdown file: Module 1.7.2 — Hash Aggregation**, using the actual experiment results.

# Module 1.7.2 — Hash Aggregation

## Objective

Understand how Apache Spark executes a `GROUP BY` aggregation using `HashAggregateExec`, why Spark typically performs aggregation in two phases, and how to validate the behavior using:

* Physical execution plans
* Partial aggregation metrics
* Shuffle metrics
* Hash probe statistics
* Memory usage
* Spill behavior
* Whole-stage code generation
* Adaptive Query Execution (AQE)
* Spark UI evidence

The goal is not simply to demonstrate that `groupBy().sum()` works.

The goal is to understand **what Spark actually executes underneath the SQL/DataFrame API**.

---

# 1. Use Case

Consider a large transaction-processing system containing billions of financial transactions.

A common analytical requirement is:

> Calculate the total transaction amount for every customer.

Conceptually:

```sql
SELECT
    customer_id,
    SUM(amount)
FROM transactions
GROUP BY customer_id;
```

At first glance this appears to be a simple aggregation.

At scale, however, Spark has to answer several engineering questions:

* Where is the aggregation performed?
* Does Spark aggregate before shuffling?
* How many records are sent across the shuffle?
* How much memory does the aggregation require?
* What happens when the number of grouping keys increases?
* Does Spark use a hash-based aggregation?
* Does AQE change the final execution?
* Does increasing `spark.sql.shuffle.partitions` necessarily increase the number of final tasks?

This experiment focuses specifically on the **hash aggregation mechanism**.

---

# 2. Engineering Question

The key question for this experiment is:

> **How does Spark's `HashAggregateExec` reduce and aggregate records before and after the shuffle?**

More specifically:

1. Does Spark use `HashAggregateExec`?
2. Does Spark perform partial aggregation before the shuffle?
3. Does the partial aggregation significantly reduce shuffle records?
4. What hash-table behavior can we observe?
5. How much memory does the aggregation consume?
6. Does the final physical plan change under AQE?
7. What evidence can we obtain from Spark UI?

---

# 3. Hypothesis

For the workload used in this experiment:

* Spark should use `HashAggregateExec`.
* Spark should perform a **partial aggregation before the Exchange**.
* Only the partially aggregated results should be shuffled.
* The final aggregation should combine partial results.
* The physical plan should contain two `HashAggregate` operators.
* Hash aggregation should require in-memory state for grouping keys and aggregate buffers.
* AQE should be able to coalesce the shuffle partitions because the resulting shuffle is relatively small.

Expected high-level plan:

```text
Range
  ↓
Project
  ↓
Partial HashAggregate
  ↓
Exchange
  ↓
Final HashAggregate
```

---

# 4. Workload

The experiment uses a deterministic synthetic transaction dataset.

| Parameter                     |         Value |
| ----------------------------- | ------------: |
| Transactions                  |    10,000,000 |
| Customers                     |       100,000 |
| Configured shuffle partitions |            20 |
| Aggregation                   | `SUM(amount)` |
| Grouping key                  | `customer_id` |
| AQE                           |       Enabled |
| Execution mode                |         Local |
| Spark version                 |         3.5.1 |
| Scala                         |       2.12.18 |
| Java runtime                  |   17.0.20.101 |

The dataset contains:

```text
10 million transactions
100 thousand customers
```

Therefore, the average number of transactions per customer is:

```text
10,000,000 / 100,000
= 100 transactions/customer
```

---

# 5. Deterministic Dataset Generation

The experiment generates the dataset using:

```scala
spark.range(0, transactionCount)
  .select(
    col("id").alias("transaction_id"),
    (col("id") % customerCount).alias("customer_id"),
    (col("id") % 1000).alias("amount")
  )
```

This gives us deterministic data.

For example:

```text
transaction_id = 0
customer_id    = 0
amount         = 0

transaction_id = 1
customer_id    = 1
amount         = 1

transaction_id = 100000
customer_id    = 0
amount         = 0
```

The deterministic structure makes the experiment reproducible and allows us to compare different aggregation experiments later.

---

# 6. Aggregation

The core operation is:

```scala
val aggregation =
  transactions
    .groupBy("customer_id")
    .agg(
      sum("amount").alias("total_amount")
    )
```

The logical operation is:

```text
GROUP BY customer_id
SUM(amount)
```

However, Spark does not simply execute this as one global operation.

Catalyst and the physical planner transform the operation into a distributed execution strategy.

---

# 7. Execution Command

Compile:

```bash
./gradlew compileScala
```

Run:

```bash
./gradlew runHashAggregationExperiment --args="10000000 100000 20 --ui-pause=300"
```

Parameters:

```text
10000000 → transaction count
100000   → customer count
20       → configured shuffle partitions
--ui-pause=300 → keep Spark UI available for 300 seconds
```

---

# 8. Spark Configuration

The experiment explicitly configures:

```text
spark.sql.shuffle.partitions = 20
```

AQE remains enabled.

The experiment also prints:

```text
spark.sql.adaptive.enabled
spark.sql.shuffle.partitions
spark.sql.codegen.aggregate.map.twolevel.enabled
spark.sql.codegen.aggregate.map.twolevel.partialOnly
```

This is important because aggregation behavior depends not only on the DataFrame code but also on Spark SQL configuration.

---

# 9. Initial Physical Plan

The initial physical plan is structurally:

```text
AdaptiveSparkPlan
+- DeserializeToObject
   +- HashAggregate
      +- Exchange
         +- HashAggregate
            +- Project
               +- Range
```

The detailed executed plan captured during the experiment was:

```text
AdaptiveSparkPlan (12)
+- == Final Plan ==
   DeserializeToObject
   +- * HashAggregate
      +- AQEShuffleRead
         +- ShuffleQueryStage
            +- Exchange
               +- * HashAggregate [partial_sum]
                  +- * Project
                  +- * Range
+- == Initial Plan ==
   DeserializeToObject
   +- HashAggregate
      +- Exchange
         +- HashAggregate
            +- Project
               +- Range
```

This plan is the most important evidence from the experiment.

---

# 10. Understanding HashAggregate

Spark represents the aggregation using:

```text
HashAggregate
```

This means Spark maintains aggregation state using hash-based structures.

Conceptually, Spark builds something similar to:

```text
customer_id → aggregate buffer
```

For example:

```text
1001 → SUM = 125000
1002 → SUM = 87300
1003 → SUM = 193400
...
```

As input rows arrive, Spark determines the grouping key and updates the corresponding aggregate buffer.

For:

```sql
GROUP BY customer_id
SUM(amount)
```

the hash aggregation state is conceptually:

```text
HashMap<customer_id, sum(amount)>
```

The actual Spark implementation is substantially more optimized than a normal JVM `HashMap`, but the conceptual model is useful.

---

# 11. Two-Phase Hash Aggregation

The most important observation is that Spark does not perform only one aggregation.

There are two aggregation stages.

## Phase 1 — Partial Aggregation

The first:

```text
HashAggregate
```

appears before the Exchange.

Conceptually:

```text
Input rows
   ↓
Local hash table
   ↓
Partial SUM per customer
```

Each upstream partition independently aggregates the rows it receives.

If a partition contains:

```text
customer 10 → 50
customer 10 → 20
customer 10 → 30
```

the local aggregation can produce:

```text
customer 10 → 100
```

instead of sending three rows through the shuffle.

---

# 12. Exchange

After partial aggregation:

```text
HashAggregate
   ↓
Exchange
```

Spark repartitions the partial results using:

```text
hashpartitioning(customer_id, 20)
```

This guarantees that records for the same customer are brought together for the final aggregation.

Conceptually:

```text
Partition 0 ─┐
Partition 1 ─┤
Partition 2 ─┤
...          ├── HashPartition(customer_id)
Partition N ─┘
                     ↓
              Final aggregation
```

---

# 13. Phase 2 — Final Hash Aggregation

After the shuffle:

```text
AQEShuffleRead
   ↓
HashAggregate
```

Spark combines the partial aggregate values.

For example:

```text
Partition A:
customer 100 → 400

Partition B:
customer 100 → 700

Partition C:
customer 100 → 300
```

The final aggregation produces:

```text
customer 100 → 1400
```

Therefore:

```text
Partial HashAggregate
        ↓
      Shuffle
        ↓
Final HashAggregate
```

is the fundamental execution pattern.

---

# 14. Why Partial Aggregation Matters

The experiment processed:

```text
10,000,000 input rows
```

but only:

```text
200,000 partial rows
```

were produced before the shuffle.

Therefore:

```text
Input rows       = 10,000,000
Partial rows     =    200,000
```

Reduction:

```text
1 - (200,000 / 10,000,000)
= 0.98
= 98%
```

So the partial aggregation reduced the number of records entering the shuffle by approximately:

# 98%

This is a major distributed-processing optimization.

---

# 15. Why Are There 200K Partial Rows?

An important Spark concept is that:

> **Partial aggregation is local to the upstream partitions.**

The source `Range` operator used:

```text
splits = Some(2)
```

Therefore the workload was initially processed by two source partitions.

There are:

```text
100,000 global customers
```

but each source partition can independently produce approximately:

```text
100,000 partial groups
```

Therefore:

```text
100,000 × 2
= 200,000 partial rows
```

This explains why:

```text
Partial output = 200K
Final output   = 100K
```

Partial rows are **not necessarily equal to the final number of distinct grouping keys**.

This distinction becomes extremely important when analyzing high-cardinality aggregations.

---

# 16. Shuffle Metrics

Observed Spark UI metrics:

| Metric                     |     Observed |
| -------------------------- | -----------: |
| Input records              |   10,000,000 |
| Partial aggregation output |      200,000 |
| Final output               |      100,000 |
| Shuffle records written    |      200,000 |
| Shuffle bytes written      |  1,963.8 KiB |
| AQE partition data         | ~2,044.2 KiB |
| Final AQE read partitions  |            1 |

The shuffle therefore carries only the partially aggregated representation.

This is fundamentally different from shuffling all 10 million input rows.

---

# 17. Shuffle Reduction

The reduction is:

```text
10,000,000
        ↓
   200,000
```

Equivalent to:

```text
98% reduction
```

This demonstrates one of the most important performance properties of distributed aggregation:

> **The effectiveness of partial aggregation directly affects shuffle volume.**

When grouping cardinality is low relative to input volume, partial aggregation can dramatically reduce network and shuffle overhead.

---

# 18. Hash Probe Metrics

Spark UI reported approximately:

```text
Partial aggregation average hash probes = 1.1
Final aggregation average hash probes   = 1.4
```

A hash probe represents work performed while locating or inserting the grouping key in the aggregation structure.

The low values indicate relatively efficient hash-table behavior for this workload.

This does **not** mean that the aggregation is free.

Spark still needs to:

* compute grouping keys
* locate hash entries
* create aggregate buffers
* update aggregate values
* manage memory
* serialize shuffle data
* merge partial results

But the probe metrics provide useful runtime evidence about the hash aggregation operator.

---

# 19. Aggregation Build Time

Observed aggregation build time was approximately:

```text
741 ms
```

This represents a significant portion of the aggregation execution work.

The important engineering observation is that the aggregation operator itself is not simply a logical abstraction.

It performs real CPU and memory work.

The workload requires Spark to maintain aggregation state for the grouping keys encountered in each partition.

---

# 20. Aggregation Memory

Observed aggregation memory:

```text
Peak aggregation memory:
~132 MiB total

Maximum task:
~66 MiB
```

No spill occurred.

```text
Spill:
0 B
```

This indicates that the aggregation state fit comfortably within the available execution memory for this workload.

---

# 21. Spill Behavior

Spark UI reported:

```text
Spill (Memory) = 0 B
Spill (Disk)   = 0 B
```

This is important.

Hash aggregation is memory-sensitive.

As the number of grouping keys increases, the aggregation state grows.

For a sufficiently high-cardinality workload, the hash map can consume substantially more memory and may eventually require spilling or fallback behavior.

This experiment therefore establishes a baseline for later cardinality experiments.

---

# 22. Sort Fallback

Observed:

```text
Sort fallback = 0
```

No sort-based fallback was observed for this workload.

This means the aggregation completed using the expected hash-based execution path without requiring the observed fallback behavior.

---

# 23. Whole-Stage Code Generation

The experiment also showed significant:

```text
WholeStageCodegen
```

activity.

Observed maximum WholeStageCodegen execution time was approximately:

```text
555 ms
```

Whole-stage code generation allows Spark to generate optimized JVM code for compatible operator pipelines instead of executing every logical operation through a high-overhead generic abstraction.

The physical plan therefore represents more than just a sequence of conceptual operators.

Spark compiles portions of that execution pipeline into efficient runtime code.

---

# 24. Adaptive Query Execution

The plan contains:

```text
AdaptiveSparkPlan
```

and:

```text
AQEShuffleRead
```

This means the query is being adaptively optimized based on runtime statistics.

The configured shuffle partition count was:

```text
20
```

However, AQE coalesced the shuffle read down to:

```text
1 partition
```

because the resulting shuffle data was small.

This is a critical distinction:

```text
Configured shuffle partitions = 20
AQE final read partitions     = 1
```

These values are not contradictory.

The Exchange is configured for 20 partitions, but AQE can combine small shuffle partitions when reading the shuffle output.

---

# 25. Three Different Partition Concepts

This experiment demonstrates three separate concepts.

## Source partitions

The `Range` operator reported:

```text
splits = Some(2)
```

So the input started with:

```text
2 source partitions
```

## Configured shuffle partitions

The Exchange was configured as:

```text
hashpartitioning(customer_id, 20)
```

Therefore:

```text
20 shuffle partitions
```

were requested.

## AQE final read partitions

AQE subsequently coalesced them to:

```text
1
```

So:

```text
Source partitions     = 2
Configured shuffle    = 20
AQE final read        = 1
```

These represent different stages of the execution.

---

# 26. Stage-Level View

The execution can be simplified to:

```text
Stage 0
Range
  ↓
Project
  ↓
Partial HashAggregate
  ↓
Shuffle Write

        ↓

Stage 1
AQE Shuffle Read
  ↓
Final HashAggregate
  ↓
Result
```

The Exchange creates the shuffle boundary.

The first stage produces the partial aggregation output.

The second stage consumes the shuffled partial results and performs the final aggregation.

---

# 27. Runtime Evidence

The observed workload produced approximately:

| Metric                   |    Value |
| ------------------------ | -------: |
| Input rows               |      10M |
| Partial rows             |     200K |
| Final rows               |     100K |
| Shuffle records          |     200K |
| Shuffle bytes            | 1.92 MiB |
| Reduction before shuffle |      98% |
| AQE final partitions     |        1 |
| Aggregation peak memory  |  132 MiB |
| Max task memory          |   66 MiB |
| Spill                    |      0 B |
| Sort fallback            |        0 |
| Partial hash probes      |      1.1 |
| Final hash probes        |      1.4 |
| Aggregation build        |  ~741 ms |

These values establish the baseline hash aggregation profile.

---

# 28. What the Spark UI Proves

The Spark UI provides strong runtime evidence for several conclusions.

### It proves:

* 10 million rows entered the workload.
* Partial aggregation reduced the data to approximately 200K records.
* 200K records were shuffled.
* Approximately 1.92 MiB of shuffle data was written.
* No spill occurred.
* Hash probe metrics were low.
* AQE coalesced the shuffle read.
* The aggregation consumed measurable execution memory.
* The physical execution used HashAggregate operators.

---

# 29. What the Spark UI Does Not Prove

The experiment is running locally.

Therefore, we should **not** conclude that:

```text
741 ms aggregation build
```

would be the same on a production cluster.

Likewise, the measured:

```text
2-second UI query duration
```

should not be interpreted as a production SLA.

Local execution is useful for understanding:

* operator behavior
* plan structure
* relative trends
* memory behavior
* shuffle mechanics
* AQE behavior

but not for predicting production throughput without cluster-level benchmarking.

---

# 30. Application Timing vs Spark UI Timing

The application-level timing and Spark UI duration should not necessarily be identical.

The application includes activities such as:

* DataFrame construction
* plan inspection
* action invocation
* result handling
* logging
* driver-side operations

The Spark UI measures execution from Spark's own job/query perspective.

Therefore:

> **Do not mix application-level elapsed time and Spark UI query duration as if they were the same metric.**

For performance analysis, use the Spark UI metrics to understand execution and use application timing only as supporting evidence.

---

# 31. Important Spark Internals

The logical operation:

```scala
groupBy("customer_id").agg(sum("amount"))
```

is transformed through several stages.

Conceptually:

```text
DataFrame API
     ↓
Logical Plan
     ↓
Catalyst Optimization
     ↓
Physical Planning
     ↓
HashAggregateExec
     ↓
Exchange
     ↓
HashAggregateExec
     ↓
WholeStageCodegen
     ↓
AQE
     ↓
Runtime execution
```

This is why a simple five-line DataFrame transformation can result in a sophisticated distributed execution plan.

---

# 32. Why Hash Aggregation Is Attractive

Hash aggregation is attractive when:

* grouping keys can be efficiently hashed
* the number of groups is manageable
* aggregation state fits in memory
* the aggregate functions can be maintained incrementally

For example:

```text
SUM
COUNT
MIN
MAX
```

can generally be maintained as compact aggregate state.

Instead of retaining every input row, Spark can maintain only the state required to produce the final result.

For:

```text
SUM(amount)
```

Spark does not need to retain every amount.

Conceptually it only needs:

```text
customer_id
+
running_sum
```

for each group.

---

# 33. Why Hash Aggregation Can Become Expensive

The advantage disappears when grouping cardinality becomes very high.

For example:

```text
10M input rows
10M distinct grouping keys
```

means there may be very little opportunity for local aggregation.

Instead of:

```text
10M rows
   ↓
200K partial rows
```

the execution may approach:

```text
10M rows
   ↓
10M partial rows
```

The aggregation then needs significantly more hash-table state and the shuffle becomes much larger.

This is the reason cardinality is one of the most important variables in aggregation performance.

Module 1.7.3 and 1.7.4 investigate this behavior further.

---

# 34. Experiment Result

The baseline hash aggregation produced:

```text
10,000,000 input rows
        ↓
200,000 partial rows
        ↓
Shuffle
        ↓
100,000 final groups
```

with:

```text
98% reduction before shuffle
```

and:

```text
0 B spill
```

The physical execution used:

```text
Partial HashAggregate
        ↓
Exchange
        ↓
Final HashAggregate
```

and AQE reduced the shuffle read from:

```text
20 → 1
```

---

# 35. Engineering Findings

## Finding 1 — Spark performs partial aggregation

The physical plan clearly shows:

```text
HashAggregate
   ↓
Exchange
```

rather than sending all input rows directly into the shuffle.

---

## Finding 2 — Partial aggregation dramatically reduces shuffle volume

The workload processed:

```text
10M rows
```

but shuffled only:

```text
200K records
```

representing approximately:

```text
98% record reduction
```

---

## Finding 3 — Hash aggregation is memory-based

The aggregation maintained in-memory state and consumed approximately:

```text
132 MiB peak
```

without spilling.

---

## Finding 4 — Cardinality determines how effective aggregation can be

The experiment's 100K-customer workload has:

```text
100 transactions/customer
```

which creates substantial opportunity for local aggregation.

Higher cardinality will reduce this benefit.

---

## Finding 5 — AQE can change the final read behavior

Although:

```text
spark.sql.shuffle.partitions = 20
```

the final AQE read used:

```text
1 partition
```

because the resulting shuffle data was small.

---

## Finding 6 — Physical plans must be validated with runtime evidence

The plan alone tells us:

```text
HashAggregate → Exchange → HashAggregate
```

but the Spark UI tells us how effectively that plan executed:

```text
10M → 200K
1.92 MiB shuffle
0 spill
1.1 / 1.4 hash probes
```

The combination is much more valuable than either source alone.

---

# 36. Baseline Hash Aggregation Profile

| Dimension                     | Observation     |
| ----------------------------- | --------------- |
| Input                         | 10M rows        |
| Grouping keys                 | 100K            |
| Average rows/key              | 100             |
| Aggregation                   | SUM             |
| Physical operator             | HashAggregate   |
| Aggregation phases            | Partial + Final |
| Partial output                | 200K            |
| Final output                  | 100K            |
| Shuffle records               | 200K            |
| Shuffle size                  | ~1.92 MiB       |
| Shuffle reduction             | 98%             |
| Configured shuffle partitions | 20              |
| AQE final read partitions     | 1               |
| Peak aggregation memory       | ~132 MiB        |
| Max task memory               | ~66 MiB         |
| Spill                         | 0 B             |
| Sort fallback                 | 0               |
| Partial hash probes           | ~1.1            |
| Final hash probes             | ~1.4            |
| Aggregation build             | ~741 ms         |

---

# 37. Execution Diagram

```text
                 10M Transactions
                        │
                        ▼
                  ┌───────────┐
                  │   Range   │
                  │ 2 splits │
                  └─────┬─────┘
                        │
                        ▼
                  ┌───────────┐
                  │  Project  │
                  └─────┬─────┘
                        │
                        ▼
             ┌────────────────────┐
             │ Partial HashAggregate│
             │ customer_id → SUM  │
             └──────────┬─────────┘
                        │
                 200K rows
                        │
                        ▼
             ┌────────────────────┐
             │      Exchange      │
             │ hashpartitioning() │
             │ 20 configured      │
             └──────────┬─────────┘
                        │
                        ▼
                 AQE Shuffle Read
                        │
                   1 final read
                    partition
                        │
                        ▼
             ┌────────────────────┐
             │ Final HashAggregate│
             │ merge partial SUMs │
             └──────────┬─────────┘
                        │
                        ▼
                  100K results
```

---

# 38. Engineering Decision

For aggregation workloads with relatively low-to-moderate grouping cardinality:

> **Allow Spark to perform partial hash aggregation before the shuffle and validate its effectiveness through runtime metrics.**

Do not evaluate aggregation performance only from the SQL/DataFrame code.

Instead, inspect:

```text
Physical Plan
      ↓
Partial aggregation
      ↓
Shuffle records
      ↓
Shuffle bytes
      ↓
Hash probes
      ↓
Aggregation memory
      ↓
Spill
      ↓
AQE behavior
```

This gives a much more reliable view of actual execution.

---

# 39. Production Relevance

The same pattern appears in production systems such as:

* Financial transaction aggregation
* Customer-level risk calculations
* Regulatory reporting
* Payment analytics
* Fraud aggregation
* Event analytics
* Account-level balances
* Daily financial summaries
* Data warehouse ETL
* Large-scale KPI computation

For example:

```text
Billions of transactions
        ↓
Partial aggregation
        ↓
Shuffle
        ↓
Final aggregation
```

The amount of data entering the shuffle can determine whether a pipeline is efficient or expensive.

---

# 40. What to Monitor in Production

For a production aggregation workload, monitor at least:

### Data characteristics

```text
Input row count
Distinct grouping keys
Rows per key
Key distribution
```

### Shuffle

```text
Shuffle records
Shuffle bytes
Partition size
Number of shuffle partitions
```

### Aggregation

```text
Hash probes
Peak memory
Spill
Sort fallback
Aggregation build time
```

### AQE

```text
Initial shuffle partitions
Final AQE partitions
Partition sizes
Skew handling
```

### End-to-end

```text
Stage duration
Task duration distribution
GC
CPU
Input/output throughput
```

---

# 41. Limitations

This experiment has several limitations.

### Local execution

The experiment runs locally rather than on a distributed Spark cluster.

Therefore:

* network behavior is limited
* executor concurrency differs from production
* local disk behavior differs
* cluster scheduling is not represented
* production-scale memory pressure is not represented

### Synthetic data

The dataset is deterministic and evenly distributed.

Real production data may contain:

* hot keys
* skew
* null-heavy keys
* uneven customer activity
* highly variable row sizes

### Small shuffle

The resulting shuffle is only approximately:

```text
1.92 MiB
```

Therefore, this is primarily an operator/internals experiment rather than a production-scale benchmark.

---

# 42. Reproducibility

Environment:

```text
Apache Spark 3.5.1
Scala 2.12.18
Java 17.0.20.101
Gradle 9.6.0
```

Run:

```bash
./gradlew compileScala
```

Then:

```bash
./gradlew runHashAggregationExperiment --args="10000000 100000 20 --ui-pause=300"
```

The workload is deterministic, allowing the experiment to be reproduced and compared with later aggregation experiments.

---

# 43. Next Experiment

The next question is:

> **How much of the aggregation work can Spark eliminate before the shuffle?**

This leads to:

# Module 1.7.3 — Partial Aggregation

The next experiment will investigate:

```text
Input
  ↓
Partial aggregation
  ↓
Shuffle
  ↓
Final aggregation
```

and measure how the effectiveness of partial aggregation changes as the number of grouping keys increases.

The key relationship will be:

```text
Cardinality
     ↓
Partial aggregation effectiveness
     ↓
Shuffle volume
     ↓
Memory
     ↓
Runtime
```

---

# 44. Final Engineering Principle

> **Hash aggregation is not merely a `GROUP BY` implementation detail. It is a distributed execution strategy that trades memory for reduced shuffle volume.**

The most important lesson from this experiment is:

```text
Understand the operator
        ↓
Understand its memory state
        ↓
Measure partial aggregation
        ↓
Measure shuffle reduction
        ↓
Measure runtime behavior
        ↓
Validate with Spark UI
```

The SQL is simple.

The execution is not.

**Don't optimize the aggregation syntax. Understand and optimize the physical execution.**
