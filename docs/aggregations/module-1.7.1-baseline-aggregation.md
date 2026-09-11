# Module 1.7.1 — Baseline Aggregation

## Experiment Objective

Establish a baseline understanding of how Apache Spark executes a simple distributed aggregation:

```text
SUM(amount) GROUP BY customer_id
```

The experiment is designed to establish the execution characteristics that will be used as a reference point for the remaining Module 1.7 aggregation experiments.

The investigation focuses on:

* How Spark plans a `groupBy + sum` aggregation
* Partial aggregation
* Final aggregation
* `HashAggregateExec`
* Shuffle boundaries
* Shuffle volume
* Aggregation memory
* Hash probe behavior
* Whole-Stage Codegen
* Adaptive Query Execution (AQE)
* Configured versus effective shuffle parallelism
* Spark UI runtime evidence

This is intentionally a baseline experiment.

The objective is **not** to optimize the workload yet.

Instead, the objective is to understand what Spark actually does before introducing changes in later experiments.

---

# Engineering Use Case

Consider a large financial transaction-processing system containing millions or billions of transaction records.

A common reporting requirement is:

> Calculate the total transaction amount for every customer.

Conceptually, the requirement is:

```sql
SELECT
    customer_id,
    SUM(amount) AS total_amount
FROM transactions
GROUP BY customer_id;
```

At small scale, this appears to be a simple aggregation.

At distributed scale, however, Spark must solve several problems:

1. Read the source data in parallel.
2. Group records locally where possible.
3. Combine records that belong to the same customer.
4. Redistribute intermediate results so that all records for a customer reach the same downstream partition.
5. Perform the final aggregation.
6. Produce the final customer-level result.

The critical distributed operation is the shuffle.

A useful high-level execution model is:

```text
Input Transactions
        |
        v
Partial Aggregation
        |
        v
      Shuffle
        |
        v
Final Aggregation
        |
        v
Customer-level Results
```

The purpose of this experiment is to observe that execution directly.

---

# Engineering Question

The primary question is:

> **How does Spark execute a simple `GROUP BY + SUM` aggregation, and how much data can it eliminate before the shuffle?**

Secondary questions include:

* Does Spark use hash aggregation?
* Does Spark perform partial aggregation?
* How many records reach the shuffle?
* How large is the shuffle?
* How much aggregation memory is required?
* Is spilling occurring?
* How does AQE change the effective number of shuffle partitions?
* What is the relationship between source partitions, configured shuffle partitions, and AQE final partitions?

---

# Hypothesis

For a standard numeric aggregation such as:

```text
SUM(amount)
GROUP BY customer_id
```

we expect Spark to generate a two-phase aggregation:

```text
Partial HashAggregate
        |
        v
     Exchange
        |
        v
Final HashAggregate
```

Because many transactions belong to the same customer, partial aggregation should significantly reduce the amount of data that needs to cross the shuffle boundary.

For this workload, we expect a large reduction in shuffle records.

---

# Experiment Workload

The baseline workload uses:

| Parameter                     |              Value |
| ----------------------------- | -----------------: |
| Transaction records           |         10,000,000 |
| Customer IDs                  |            100,000 |
| Shuffle partitions configured |                 20 |
| Aggregation                   |      `SUM(amount)` |
| Grouping key                  |      `customer_id` |
| AQE                           |            Enabled |
| Execution environment         |      Local Windows |
| Source                        | `spark.range(...)` |

The average number of transactions per customer is:

```text
10,000,000 / 100,000
= 100 transactions/customer
```

This relatively high repetition of customer IDs gives partial aggregation substantial opportunity to reduce records before the shuffle.

---

# Dataset Generation

The experiment generates a deterministic dataset using:

```scala
spark.range(0, transactionCount)
  .select(
    col("id").alias("transaction_id"),
    (col("id") % customerCount).alias("customer_id"),
    (col("id") % 1000).alias("amount")
  )
```

The generated columns are:

| Column           | Meaning                                        |
| ---------------- | ---------------------------------------------- |
| `transaction_id` | Unique transaction identifier                  |
| `customer_id`    | Deterministically generated customer ID        |
| `amount`         | Deterministically generated transaction amount |

For the baseline configuration:

```text
transactionCount = 10,000,000
customerCount    = 100,000
```

Therefore:

```text
customer_id = id % 100000
```

and:

```text
amount = id % 1000
```

The deterministic data generation makes the experiment reproducible.

---

# Spark Configuration

The experiment configures:

```text
spark.sql.shuffle.partitions = 20
```

AQE remains enabled through the enterprise Spark session configuration.

The experiment therefore allows us to observe both:

```text
configured shuffle partitioning
```

and:

```text
adaptive runtime partitioning
```

---

# Execution Command

The baseline experiment was executed using:

```bash
./gradlew runBaselineAggregationExperiment --args="10000000 100000 20 --ui-pause=300"
```

The `--ui-pause=300` argument keeps the Spark application alive for 300 seconds after execution so that the Spark UI can be inspected before the application terminates.

---

# Aggregation Definition

The logical aggregation is equivalent to:

```sql
SELECT
    customer_id,
    SUM(amount) AS total_amount
FROM transactions
GROUP BY customer_id;
```

The important point is that the logical SQL does not describe the distributed execution details.

Spark's physical planner transforms the logical aggregation into executable operators.

---

# Observed Physical Plan

The observed physical plan was:

```text
AdaptiveSparkPlan (6)
+- HashAggregate (5)
   +- Exchange (4)
      +- HashAggregate (3)
         +- Project (2)
            +- Range (1)
```

The execution can therefore be represented as:

```text
Range
  |
  v
Project
  |
  v
Partial HashAggregate
  |
  v
Exchange
  |
  v
Final HashAggregate
```

This is the fundamental execution pattern for this aggregation.

---

# Operator-by-Operator Analysis

## 1. Range

The source was:

```text
Range (0, 10000000, step=1, splits=Some(2))
```

This means Spark generated:

```text
10,000,000 input records
```

using:

```text
2 source/input partitions
```

This distinction becomes important later.

The source parallelism is:

```text
2
```

It is **not** the same as:

```text
spark.sql.shuffle.partitions = 20
```

---

# 2. Project

The Project derives the required columns:

```text
transaction_id
customer_id
amount
```

The Project itself does not introduce a shuffle.

It transforms the records produced by the Range source.

---

# 3. Partial HashAggregate

This is one of the most important operators in the experiment.

Spark does not immediately shuffle all 10 million transaction records.

Instead, each upstream partition performs a local aggregation.

Conceptually:

```text
Input rows
    |
    v
Hash table
    |
    v
customer_id -> running SUM(amount)
```

For each input row Spark determines the corresponding grouping key.

If the key already exists in the local aggregation state, Spark updates the running sum.

If it does not exist, Spark creates a new aggregation entry.

The resulting intermediate dataset contains partial sums.

---

# Partial Aggregation Output

The Spark UI showed:

```text
Input rows:
10,000,000
```

and:

```text
Partial HashAggregate output:
200,000
```

Therefore only:

```text
200,000
```

records needed to cross the shuffle boundary.

This is a critical result.

---

# Partial Aggregation Reduction

The reduction can be calculated as:

```text
reduction
= 1 - (partial rows / input rows)
```

Substituting the observed values:

```text
= 1 - (200,000 / 10,000,000)

= 1 - 0.02

= 0.98

= 98%
```

Therefore:

```text
Input records          = 10,000,000
Partial records        = 200,000
Records eliminated     = 9,800,000
Reduction              = 98%
```

Only approximately:

```text
2%
```

of the original input record count entered the shuffle.

This is one of the strongest observations from the baseline experiment.

---

# Why Are There 200K Partial Rows Instead of 100K?

There are:

```text
100,000 distinct customer IDs
```

but the partial aggregation produced:

```text
200,000 rows
```

This is expected.

The reason is that partial aggregation occurs independently within each upstream partition.

The source had:

```text
2 input partitions
```

Conceptually:

```text
Input Partition 1
-----------------
100K customer groups
        |
        v
100K partial rows


Input Partition 2
-----------------
100K customer groups
        |
        v
100K partial rows
```

Combined:

```text
100K + 100K
= 200K partial rows
```

The same customer can therefore appear in more than one partial aggregation output.

The shuffle is responsible for bringing those partial values together.

This leads to an important distinction:

```text
Global distinct keys
        !=
Partial aggregation output rows
```

Partial aggregation output is dependent on both:

* grouping-key cardinality
* upstream partitioning

---

# 4. Exchange

After partial aggregation, Spark performs:

```text
Exchange
```

using hash partitioning on:

```text
customer_id
```

The observed partitioning was:

```text
hashpartitioning(customer_id, 20)
```

The purpose of this shuffle is to guarantee that records for the same customer are routed to the same downstream partition.

Conceptually:

```text
Partial results
       |
       v
hash(customer_id)
       |
       v
Shuffle partitions
       |
       v
Same customer -> same downstream partition
```

This allows the final aggregation to safely combine all partial sums for each customer.

---

# 5. Final HashAggregate

After the Exchange, Spark performs the final aggregation.

The final stage combines the partial results:

```text
Partial customer sums
        |
        v
Final HashAggregate
        |
        v
One result per customer
```

The expected final cardinality is:

```text
100,000 customer groups
```

The Spark UI showed:

```text
Final aggregation output = 100,000 rows
```

Therefore the aggregation successfully reduced the dataset from:

```text
10,000,000 transactions
```

to:

```text
100,000 customer-level results
```

---

# End-to-End Data Reduction

The complete logical reduction is:

```text
10,000,000 input transactions
            |
            | Partial aggregation
            v
   200,000 partial records
            |
            | Shuffle
            v
   Final HashAggregate
            |
            v
     100,000 results
```

There are therefore two different reductions worth understanding.

### Reduction before shuffle

```text
10M → 200K
```

or:

```text
98%
```

### Final aggregation

```text
200K → 100K
```

The first reduction is particularly important for distributed performance because it directly reduces shuffle record volume.

---

# Spark UI — Query-Level Results

The Spark UI reported:

| Metric                     | Observed Value |
| -------------------------- | -------------: |
| Query duration             |     ~2 seconds |
| Input rows                 |     10,000,000 |
| Partial aggregation output |        200,000 |
| Final aggregation output   |        100,000 |
| Shuffle records written    |        200,000 |
| Shuffle bytes written      |    1,963.8 KiB |
| Spill                      |            0 B |
| Sort fallback              |              0 |
| AQE final read partitions  |              1 |

The application-level action timing was approximately:

```text
2.222 seconds
```

The application-level timing and Spark UI query duration should not be treated as identical metrics.

The UI query duration measures Spark SQL execution from the Spark SQL perspective, while the application timer includes surrounding application-level execution.

---

# Shuffle Metrics

Observed:

```text
Shuffle records written = 200,000
```

and:

```text
Shuffle bytes written = 1,963.8 KiB
```

Approximately:

```text
1.92 MiB
```

of shuffle data was generated.

This is relatively small compared with the original 10 million-row input.

The important point is not merely the number of bytes.

The important point is that partial aggregation reduced the number of records crossing the network/shuffle boundary from:

```text
10,000,000
```

to:

```text
200,000
```

---

# Shuffle Write Time

Observed:

| Metric                          |     Value |
| ------------------------------- | --------: |
| Total shuffle write time        |     64 ms |
| Maximum task shuffle write time |     33 ms |
| Maximum shuffle bytes per task  | 981.9 KiB |

The maximum task wrote approximately:

```text
981.9 KiB
```

of shuffle data.

The shuffle was therefore not the dominant cost in this local workload.

---

# Shuffle Read

The Spark UI showed:

```text
Remote bytes read = 0
Remote blocks = 0
```

while:

```text
Local bytes read ≈ 1.92 MiB
```

The local execution environment means the shuffle data remained local to the machine.

This is important when interpreting the results.

A production cluster would typically involve:

```text
network transfer
remote shuffle blocks
multiple executors
```

which can make shuffle cost much more significant.

Therefore the local experiment is primarily demonstrating Spark's execution mechanics rather than measuring production network performance.

---

# Aggregation Hash Probe Metrics

Spark UI reported average hash probes of approximately:

| Aggregation           | Average Hash Probes |
| --------------------- | ------------------: |
| Partial HashAggregate |                 1.1 |
| Final HashAggregate   |                 1.4 |

These values provide evidence about hash-table lookup behavior.

A probe occurs when the aggregation implementation searches for the appropriate grouping-key entry.

For example:

```text
customer_id
     |
     v
hash lookup
     |
     +---- existing key -> update aggregate
     |
     +---- new key      -> create aggregate entry
```

The observed values are low and do not indicate problematic hash-table behavior.

---

# Aggregation Build Time

Spark UI reported:

```text
Time in aggregation build
≈ 741 ms total
```

with a maximum task value of approximately:

```text
557? 
```

The most reliable captured aggregation metric from the run was approximately:

```text
741 ms total
```

The workload completed without spilling.

The aggregation build time represents the work involved in maintaining the aggregation state and processing the grouping keys.

---

# Aggregation Memory

Observed:

| Metric                        |   Value |
| ----------------------------- | ------: |
| Peak aggregation memory total | 132 MiB |
| Maximum per-task peak memory  |  66 MiB |
| Spill                         |     0 B |

The maximum observed task-level aggregation memory was:

```text
66 MiB
```

This indicates that the aggregation state fit comfortably into memory for this workload.

No disk spill was observed.

---

# Spill Behavior

The Spark UI reported:

```text
Spill size = 0 B
```

This is important because aggregation operators can become memory-intensive when grouping cardinality becomes large.

In this baseline:

```text
100K grouping keys
```

did not cause observable spilling.

Later cardinality experiments will deliberately increase the number of grouping keys to investigate this behavior.

---

# Sort Fallback

The Spark UI reported:

```text
Sort fallback tasks = 0
```

Therefore there was no observed fallback from the normal hash aggregation path to a sort-based fallback mechanism.

This supports the conclusion that the baseline workload was comfortably handled by the hash aggregation implementation.

---

# Whole-Stage Code Generation

The Spark UI reported approximately:

```text
WholeStageCodegen total = 1.1 seconds
```

with a maximum observed task contribution of approximately:

```text
557 ms
```

Whole-Stage Codegen allows Spark to generate optimized JVM code for compatible operator pipelines.

The physical execution therefore does not simply execute each logical operation independently through generic interpreter calls.

Instead, Spark can fuse compatible operators into generated code.

Conceptually:

```text
Range
  +
Project
  +
Partial HashAggregate
```

can be executed through generated JVM code.

---

# AQE Behavior

The physical plan begins with:

```text
AdaptiveSparkPlan
```

indicating that Adaptive Query Execution is enabled.

The initial aggregation Exchange was configured for:

```text
20 shuffle partitions
```

However, the final AQE execution showed:

```text
Original shuffle partitions = 20
Coalesced read partitions   = 1
```

The AQE partition data size was approximately:

```text
2,044.2 KiB
```

This demonstrates one of the most important Spark concepts:

> The configured shuffle partition count is not necessarily the final number of partitions actually processed by downstream tasks.

---

# Configured vs Effective Parallelism

There are three different partition concepts in this experiment.

## 1. Source/Input Partitions

The Range source reported:

```text
splits=Some(2)
```

Therefore:

```text
Input partitions = 2
```

---

## 2. Configured Shuffle Partitions

The Exchange was configured as:

```text
spark.sql.shuffle.partitions = 20
```

Therefore:

```text
Configured shuffle partitions = 20
```

---

## 3. AQE Effective Read Partitions

AQE coalesced the small shuffle output to:

```text
1 partition
```

Therefore:

```text
Input partitions              = 2
Configured shuffle partitions = 20
AQE final read partitions     = 1
```

These numbers are not contradictory.

They represent different points in Spark's execution pipeline.

---

# Why Did AQE Coalesce 20 Partitions to 1?

The workload generated only approximately:

```text
1.92 MiB
```

of shuffle data.

Maintaining 20 downstream tasks for such a small dataset would provide little benefit.

AQE can inspect runtime shuffle statistics and coalesce small partitions.

Conceptually:

```text
Configured
20 shuffle partitions
        |
        v
Runtime shuffle statistics
        |
        v
AQE
        |
        v
1 effective read partition
```

This is an important reason why configured partition counts should not be interpreted as actual runtime parallelism.

---

# Stage Structure

The execution consisted of a shuffle map stage followed by a result stage.

The source side had:

```text
2 tasks
```

because the Range source contained:

```text
2 input partitions
```

The downstream AQE execution ultimately processed the small shuffle output with reduced effective parallelism.

The important execution boundary is:

```text
Stage 0
Range
Project
Partial HashAggregate
        |
        | Shuffle
        v
Stage 1
AQE Shuffle Read
Final HashAggregate
```

The exact stage IDs can vary between runs, but the execution architecture remains the same.

---

# Spark UI — Task-Level Evidence

The captured UI metrics included:

| Metric                          |    Observed |
| ------------------------------- | ----------: |
| Source data size total          |    ~4.6 MiB |
| Maximum source task data size   |    ~2.3 MiB |
| Shuffle bytes total             | 1,963.8 KiB |
| Maximum shuffle bytes/task      |   981.9 KiB |
| Shuffle write time total        |       64 ms |
| Maximum shuffle write/task      |       33 ms |
| Remote bytes read               |           0 |
| Remote blocks read              |           0 |
| Local blocks read               |           2 |
| Spill                           |         0 B |
| Sort fallback                   |           0 |
| Partial average hash probes     |         1.1 |
| Final average hash probes       |         1.4 |
| Peak aggregation memory total   |     132 MiB |
| Maximum task aggregation memory |      66 MiB |

These metrics provide stronger evidence than the application-level elapsed time alone.

---

# What the Spark UI Proves

The UI provides direct evidence that:

### 1. Partial aggregation occurred

```text
10M input
↓
200K partial rows
```

### 2. A shuffle occurred

```text
Exchange
```

with:

```text
20 configured partitions
```

### 3. The shuffle was small

```text
1,963.8 KiB
```

### 4. Aggregation stayed in memory

```text
Spill = 0
```

### 5. Hash aggregation operated normally

```text
Partial probes = 1.1
Final probes   = 1.4
```

### 6. AQE changed effective parallelism

```text
20 configured
↓
1 effective AQE read partition
```

---

# What the Spark UI Does NOT Prove

The experiment should not be interpreted as proof that:

* 20 shuffle partitions is always optimal.
* One final partition is optimal for production.
* Hash aggregation will always consume only 66 MiB.
* A 2-second query duration will occur in production.
* Shuffle is always inexpensive.
* Local execution represents a distributed production cluster.

The experiment is a controlled baseline.

Its purpose is to establish the mechanics and measurements that later experiments can build upon.

---

# Local Execution Limitation

The experiment was executed on a local Windows environment.

Therefore:

```text
Remote shuffle bytes = 0
```

and:

```text
Remote shuffle blocks = 0
```

This is fundamentally different from a distributed Spark cluster.

In production:

```text
Executor A
      |
      | network
      v
Executor B
```

may be involved in shuffle data transfer.

Therefore the absolute timing numbers should not be used as production capacity estimates.

The physical plan and relative mechanics are the more important lessons from this experiment.

---

# Important Distinction: Application Timing vs Spark UI Timing

The application measured approximately:

```text
2.222 seconds
```

for the action.

The Spark UI showed approximately:

```text
2 seconds
```

for the SQL query.

These values should not be expected to match exactly.

Application timing may include:

* DataFrame construction
* action invocation
* listener overhead
* application-side timing
* other surrounding code

while the Spark SQL UI measures query execution from Spark's perspective.

For future experiments:

> Use Spark UI metrics for execution analysis and application timing as supporting evidence.

---

# Engineering Interpretation

The baseline establishes a very important execution model.

The SQL:

```sql
SELECT
    customer_id,
    SUM(amount)
FROM transactions
GROUP BY customer_id;
```

becomes approximately:

```text
Range
  |
  v
Project
  |
  v
Partial HashAggregate
  |
  | 200K records
  v
Exchange
  |
  | 20 configured partitions
  v
AQE Shuffle Read
  |
  | 1 effective read partition
  v
Final HashAggregate
  |
  v
100K customer results
```

The logical operation is simple.

The distributed execution is not.

---

# Engineering Findings

## Finding 1 — Spark uses a two-phase aggregation

The observed execution is:

```text
Partial HashAggregate
        |
        v
Exchange
        |
        v
Final HashAggregate
```

This is the fundamental aggregation execution pattern demonstrated by the experiment.

---

## Finding 2 — Partial aggregation provides major shuffle reduction

The workload begins with:

```text
10,000,000 records
```

but only:

```text
200,000 records
```

enter the shuffle.

That represents:

```text
98% record reduction
```

before the shuffle.

This is one of the most important performance mechanisms in Spark aggregation.

---

## Finding 3 — Partial aggregation output depends on upstream partitioning

There are:

```text
100,000 global customer IDs
```

but:

```text
200,000 partial rows
```

because the source has:

```text
2 input partitions
```

Therefore:

```text
partial output rows
```

should not be confused with:

```text
global distinct grouping keys
```

---

## Finding 4 — Shuffle volume is driven by partial output

The shuffle contains:

```text
200,000 records
```

rather than:

```text
10,000,000 records
```

This demonstrates why partial aggregation is so important.

If partial aggregation becomes less effective, shuffle volume can increase dramatically.

That question is investigated in Module 1.7.3 and Module 1.7.4.

---

## Finding 5 — Aggregation fit comfortably in memory

Observed:

```text
Maximum task memory = 66 MiB
Spill = 0 B
```

There is no evidence of memory pressure or spilling for this workload.

---

## Finding 6 — Hash aggregation behaved efficiently

Observed average probes:

```text
Partial = 1.1
Final   = 1.4
```

There is no evidence of problematic hash-table behavior.

---

## Finding 7 — AQE changes effective partitioning

The Exchange was configured with:

```text
20 partitions
```

but AQE reduced the effective read to:

```text
1 partition
```

This demonstrates:

```text
configured shuffle partitions
        !=
actual downstream execution partitions
```

---

## Finding 8 — Source parallelism is independent of shuffle partition configuration

The source used:

```text
2 input partitions
```

while the Exchange used:

```text
20 shuffle partitions
```

Therefore changing:

```text
spark.sql.shuffle.partitions
```

does not automatically change source/input parallelism.

---

# Baseline Performance Profile

The complete baseline profile is:

| Category                      | Observed Value |
| ----------------------------- | -------------: |
| Input records                 |     10,000,000 |
| Distinct customers            |        100,000 |
| Average rows/customer         |            100 |
| Source partitions             |              2 |
| Configured shuffle partitions |             20 |
| Partial aggregation rows      |        200,000 |
| Final aggregation rows        |        100,000 |
| Shuffle records               |        200,000 |
| Shuffle bytes                 |    1,963.8 KiB |
| Shuffle reduction             |            98% |
| AQE final read partitions     |              1 |
| AQE partition data            |    2,044.2 KiB |
| Peak aggregation memory total |        132 MiB |
| Maximum task memory           |         66 MiB |
| Partial average hash probes   |            1.1 |
| Final average hash probes     |            1.4 |
| Spill                         |            0 B |
| Sort fallback                 |              0 |
| Shuffle write time total      |          64 ms |
| Maximum shuffle write time    |          33 ms |
| WholeStageCodegen total       |         ~1.1 s |
| Application action timing     |       ~2.222 s |
| Spark UI query duration       |           ~2 s |
| Remote shuffle bytes          |              0 |
| Remote shuffle blocks         |              0 |

---

# Baseline Execution Diagram

```text
                         10M Transactions
                                |
                                v
                    +----------------------+
                    |        Range         |
                    |  2 input partitions  |
                    +----------+-----------+
                               |
                               v
                    +----------------------+
                    |       Project        |
                    | customer_id, amount  |
                    +----------+-----------+
                               |
                               v
                    +----------------------+
                    | Partial HashAggregate |
                    |                      |
                    | 10M -> 200K rows     |
                    +----------+-----------+
                               |
                               | Shuffle
                               | 200K records
                               | ~1.92 MiB
                               v
                    +----------------------+
                    |       Exchange       |
                    | hashpartitioning(     |
                    | customer_id, 20)     |
                    +----------+-----------+
                               |
                               v
                    +----------------------+
                    |    AQE Shuffle Read   |
                    |                      |
                    | 20 -> 1 read partition|
                    +----------+-----------+
                               |
                               v
                    +----------------------+
                    | Final HashAggregate   |
                    |                      |
                    | 200K -> 100K rows    |
                    +----------+-----------+
                               |
                               v
                      100K Customer Results
```

---

# Engineering Decision

For a standard Spark aggregation:

```text
GROUP BY key
SUM(value)
```

the expected physical pattern should generally be:

```text
Partial HashAggregate
        |
        v
Exchange
        |
        v
Final HashAggregate
```

Before attempting optimization, capture the following baseline metrics:

1. Input row count
2. Grouping-key cardinality
3. Average rows per key
4. Partial aggregation output
5. Shuffle records
6. Shuffle bytes
7. Aggregation memory
8. Hash probe behavior
9. Spill
10. Sort fallback
11. Source/input partitions
12. Configured shuffle partitions
13. AQE final partitions
14. Task-level shuffle metrics
15. Execution time

Without this baseline, later tuning results cannot be interpreted reliably.

---

# What We Will Investigate Next

This baseline becomes the control point for the remaining Module 1.7 experiments.

### Module 1.7.2 — Hash Aggregation

Investigate the internals of `HashAggregateExec`, including:

* hash aggregation behavior
* hash probes
* aggregation memory
* Whole-Stage Codegen
* AQE interaction

---

### Module 1.7.3 — Partial Aggregation

Investigate how partial aggregation affects:

* shuffle records
* shuffle bytes
* aggregation cost
* memory consumption

and how those effects change with grouping cardinality.

---

### Module 1.7.4 — Aggregation Cardinality

Keep input volume constant while increasing grouping-key cardinality:

```text
100K
500K
1M
5M
10M
```

This will demonstrate that:

> The same physical plan can behave very differently depending on data cardinality.

---

### Module 1.7.5 — Shuffle Partition Sizing

Vary:

```text
spark.sql.shuffle.partitions
```

across:

```text
10
20
50
100
200
```

and investigate:

* physical Exchange partition count
* shuffle volume
* task parallelism
* AQE coalescing
* partition sizes
* shuffle write behavior

---

# Production Relevance

In a production data platform, a query that appears as:

```sql
GROUP BY customer_id
```

should not be evaluated solely from the SQL.

A senior data engineer should ask:

```text
How many input records?
How many distinct keys?
How many rows per key?
How much partial reduction?
How much shuffle?
How large are the shuffle partitions?
How much aggregation memory?
Any spill?
Any skew?
How many effective AQE partitions?
How much source parallelism?
```

This is the difference between:

```text
SQL-level optimization
```

and:

```text
physical execution optimization
```

---

# Reproducibility

The experiment is deterministic and can be reproduced with:

```bash
./gradlew runBaselineAggregationExperiment --args="10000000 100000 20 --ui-pause=300"
```

The key workload parameters are:

```text
transactions = 10,000,000
customers    = 100,000
partitions   = 20
```

---

# Limitations

This experiment has several deliberate limitations.

## 1. Local Execution

The workload runs on a local Windows environment.

Therefore:

```text
remote shuffle = 0
```

and network behavior is not representative of a distributed production cluster.

---

## 2. Small Input Parallelism

The Range source used:

```text
2 input partitions
```

Production workloads may have hundreds or thousands of source partitions.

---

## 3. Deterministic Distribution

Customer IDs are generated using:

```text
id % customerCount
```

This creates a controlled distribution.

Real production data can have:

* skew
* hot keys
* long-tail distributions
* temporal concentration
* uneven customer activity

---

## 4. Single Local Run

Application timings should not be treated as statistically significant benchmarks.

For production performance decisions, use:

* repeated runs
* representative cluster sizes
* production-like data volume
* realistic data distribution
* Spark UI metrics
* executor metrics
* GC metrics
* spill metrics

---

# Final Engineering Principle

The most important lesson from Module 1.7.1 is:

> **Do not optimize a Spark aggregation by looking only at the SQL. First understand the physical execution and measure how much data survives partial aggregation before the shuffle.**

For this baseline:

```text
10,000,000 input rows
          |
          v
200,000 partial rows
          |
          v
1,963.8 KiB shuffle
          |
          v
100,000 final groups
```

with:

```text
98% pre-shuffle record reduction
0 B spill
0 sort-fallback tasks
66 MiB maximum task aggregation memory
1.1 partial hash probes
1.4 final hash probes
20 configured shuffle partitions
1 AQE final read partition
2 source partitions
```

This is the baseline against which the subsequent aggregation experiments should be compared.

**Core engineering principle:**

> **Understand the physical plan → measure the shuffle → measure aggregation state → inspect AQE → then optimize.**
