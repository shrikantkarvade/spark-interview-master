Yes. Here is the **complete `module-1.7.9-data-skew-in-aggregation.md`**, clean and copy-paste ready, in **one single code block**.

````markdown
# Module 1.7.9 — Data Skew in Aggregation

## Experiment Objective

Understand how **data skew in aggregation keys** affects Apache Spark execution.

This module investigates:

- What aggregation-key skew is
- Why one or a few hot keys can create straggler tasks
- How `Exchange hashpartitioning(...)` distributes aggregation keys
- Why increasing `spark.sql.shuffle.partitions` does not automatically solve hot-key skew
- How partial aggregation reduces data before shuffle
- How skew appears in the Spark UI
- What Adaptive Query Execution (AQE) can and cannot solve
- How salting can distribute a hot aggregation key
- The correctness and performance trade-offs of salting
- How a senior engineer should diagnose and mitigate aggregation skew in production

The goal is not simply to make Spark faster.

The goal is to understand **why the workload becomes slow, prove the bottleneck with Spark evidence, apply an appropriate mitigation, and validate that the mitigation is actually better.**

---

# 1. Business / Production Use Case

Consider a financial transaction platform processing billions of transactions.

A common requirement is:

```text
Calculate total transaction amount and transaction count per customer.
````

The logical operation is:

```scala
transactions
  .groupBy("customer_id")
  .agg(
    sum("amount"),
    count("*")
  )
```

In a healthy dataset, transaction volume is reasonably distributed across customers.

However, production data may contain extremely large customers or abnormal keys such as:

```text
UNKNOWN
NULL
DEFAULT
SYSTEM
TEST
```

For example:

| customer_id | Approximate rows |
| ----------- | ---------------: |
| C000001     |              100 |
| C000002     |              120 |
| C000003     |               90 |
| ...         |              ... |
| C999999     |              110 |
| UNKNOWN     |        9,000,000 |

The logical query is still:

```text
GROUP BY customer_id
```

But the physical execution can become highly imbalanced.

One shuffle partition may receive the majority of the rows belonging to the hot key.

That creates a **straggler task**.

---

# 2. What Is Data Skew?

Data skew occurs when the distribution of data across partitions is highly uneven.

For an aggregation:

```text
GROUP BY customer_id
```

Spark needs all rows belonging to the same grouping key to eventually reach the same logical aggregation group.

Conceptually:

```text
customer_id
     |
     v
hash(customer_id)
     |
     v
shuffle partition
```

If one customer has a disproportionate amount of data:

```text
Normal:

Customer A -> 100 rows
Customer B -> 120 rows
Customer C -> 110 rows
Customer D -> 90 rows

Hot key:

UNKNOWN -> 9,000,000 rows
```

then the corresponding shuffle partition can become much larger than the others.

The result may look like:

```text
Partition 0   120 MB
Partition 1   115 MB
Partition 2   118 MB
Partition 3   121 MB
Partition 4   119 MB
Partition 5  8,700 MB   <-- hot partition
Partition 6   116 MB
Partition 7   122 MB
```

The overall Spark job may have many fast tasks and one extremely slow task.

This is one of the classic causes of:

```text
Most tasks completed
        |
        v
One task remains
        |
        v
Stage waits
        |
        v
Job completion delayed
```

---

# 3. Why Aggregation Skew Is Different From Normal Partition Imbalance

It is important to distinguish:

```text
Too few partitions
```

from:

```text
One or more hot keys
```

Increasing the number of shuffle partitions can help when partitions are simply too large.

For example:

```text
32 partitions
      |
      v
64 partitions
      |
      v
128 partitions
```

may reduce the average amount of data processed by each partition.

However, a single grouping key still belongs to one logical grouping key.

For example:

```text
customer_id = UNKNOWN
```

will hash to the same grouping partition for a normal hash-based exchange.

Therefore:

```text
32 partitions
```

to:

```text
256 partitions
```

does not automatically transform:

```text
UNKNOWN -> one logical aggregation group
```

into:

```text
UNKNOWN -> many independent final groups
```

That distinction is critical.

---

# 4. Aggregation Execution Model

A typical Spark aggregation can contain:

```text
Input
  |
  v
Partial HashAggregate
  |
  v
Exchange hashpartitioning(customer_id)
  |
  v
Final HashAggregate
```

Conceptually:

```text
                    +----------------------+
                    |      Input Data      |
                    +----------+-----------+
                               |
                               v
                    +----------------------+
                    | Partial HashAggregate|
                    +----------+-----------+
                               |
                               v
                    +----------------------+
                    |        Exchange      |
                    | hashpartitioning(...)|
                    +----------+-----------+
                               |
              +----------------+----------------+
              |                |                |
              v                v                v
          Partition 0      Partition 1      Partition N
              |                |                |
              +----------------+----------------+
                               |
                               v
                    +----------------------+
                    |  Final HashAggregate |
                    +----------------------+
```

Partial aggregation is important because Spark can combine rows locally before the shuffle.

For example:

```text
1000 transactions for customer A
```

may become:

```text
customer A -> subtotal + count
```

before shuffle.

This reduces shuffle volume.

But partial aggregation does **not automatically eliminate key skew**.

If one key has enormous cardinality in the input, the downstream grouping for that key can still dominate execution.

---

# 5. Experiment Design

The experiment should compare several distributions.

Recommended dataset:

```text
Rows                 = 10,000,000
Customers            = 1,000,000
Shuffle partitions   = 32
```

Test cases:

| Experiment    | Hot-key distribution |
| ------------- | -------------------: |
| Uniform       |  No significant skew |
| Moderate skew |                  90% |
| Severe skew   |                  99% |
| Extreme skew  |                99.9% |

The exact numbers should be confirmed from the actual experiment output.

Do not assume a performance improvement before measuring it.

---

# 6. Baseline Aggregation

The baseline aggregation is:

```scala
val result = df
  .groupBy("customer_id")
  .agg(
    sum("amount").alias("total_amount"),
    count("*").alias("transaction_count")
  )
```

The purpose is to establish the normal execution pattern before introducing skew.

The baseline should capture:

* Execution time
* Number of output rows
* Shuffle read
* Shuffle write
* Number of tasks
* Maximum task duration
* Median task duration
* Physical plan
* AQE behavior

---

# 7. Uniform Dataset

A uniform dataset attempts to distribute transactions reasonably evenly among customers.

Conceptually:

```text
Customer A -> similar number of rows
Customer B -> similar number of rows
Customer C -> similar number of rows
...
```

Expected behavior:

```text
Task durations

Task 1  █████████
Task 2  ████████
Task 3  █████████
Task 4  ████████
Task 5  █████████
Task 6  ████████
```

The task-duration distribution should be relatively balanced.

There can still be normal variation due to:

* JVM scheduling
* garbage collection
* OS scheduling
* input differences
* executor variability

The goal is not perfect equality.

The goal is to identify whether there is a major outlier.

---

# 8. Skewed Dataset

Now create a distribution where most rows belong to one key.

Example:

```text
customer_id = HOT_CUSTOMER
```

Suppose:

```text
Total rows = 10,000,000
```

and:

```text
HOT_CUSTOMER = 9,900,000 rows
Other customers = 100,000 rows
```

The logical aggregation remains:

```scala
df
  .groupBy("customer_id")
  .agg(
    sum("amount"),
    count("*")
  )
```

The query has not changed.

Only the data distribution has changed.

This is important because it demonstrates that:

> The same logical query can have dramatically different physical runtime behavior depending on the data distribution.

---

# 9. Expected Physical Plan

The physical plan should contain a structure similar to:

```text
AdaptiveSparkPlan
+- HashAggregate
   +- Exchange hashpartitioning(customer_id, 32)
      +- HashAggregate
         +- ...
```

The exact plan may differ depending on:

* Spark version
* AQE configuration
* aggregation expressions
* input source
* optimizer decisions

Do not treat a conceptual plan as the exact expected output.

The actual physical plan from the experiment is the authoritative evidence.

---

# 10. What to Inspect in the Physical Plan

Look specifically for:

```text
HashAggregate
Exchange
hashpartitioning(customer_id, ...)
AdaptiveSparkPlan
```

Questions to answer:

### Question 1

Where does the shuffle occur?

Look for:

```text
Exchange
```

### Question 2

What is the partitioning expression?

For example:

```text
hashpartitioning(customer_id, 32)
```

### Question 3

Are partial and final aggregation stages present?

Look for:

```text
HashAggregate
    |
Exchange
    |
HashAggregate
```

### Question 4

Is AQE active?

Look for:

```text
AdaptiveSparkPlan
```

and inspect the Spark UI for runtime changes.

---

# 11. Spark UI Investigation

Run the experiment and open:

```text
http://localhost:4040
```

Then navigate to:

```text
SQL
    |
    +-- Aggregation Query
            |
            +-- Details
```

Inspect the execution stages.

The most important evidence is the task-duration distribution.

---

# 12. Task Duration Is the Most Important Skew Signal

Suppose the tasks look like:

```text
Task 1     2.1 sec
Task 2     2.3 sec
Task 3     2.0 sec
Task 4     2.2 sec
Task 5     2.1 sec
Task 6    38.7 sec   <-- suspicious
Task 7     2.2 sec
Task 8     2.0 sec
```

This is a classic straggler pattern.

The key question is:

```text
Why is Task 6 processing so much more data?
```

Inspect:

* Input size
* Shuffle read
* Shuffle write
* Records read
* Records written
* Task duration
* Peak memory
* Spill metrics

The goal is to correlate:

```text
Large data volume
        +
Long task duration
        +
One/few outlier tasks
```

with the skewed key distribution.

---

# 13. Useful Skew Metric

A simple diagnostic metric is:

```text
Skew Ratio =
Maximum Task Duration / Median Task Duration
```

For example:

```text
Maximum task duration = 40 seconds
Median task duration  = 2 seconds

Skew ratio = 40 / 2
           = 20x
```

A high ratio is a strong signal that a small number of tasks are stragglers.

This is not a universal production threshold.

It is a diagnostic metric for comparing experiments.

---

# 14. Another Useful Metric: Data Size Imbalance

Compare:

```text
Maximum partition data
```

with:

```text
Median partition data
```

For example:

```text
Maximum partition = 8.5 GB
Median partition  = 120 MB
```

Then:

```text
Data skew ratio
= 8.5 GB / 120 MB
≈ 70.8x
```

This provides direct evidence that the problem is data distribution rather than simply a slow executor.

---

# 15. Why More Shuffle Partitions May Not Solve Hot-Key Skew

Consider:

```text
spark.sql.shuffle.partitions = 32
```

and:

```text
customer_id = HOT_CUSTOMER
```

All rows for that grouping key must ultimately be combined into the same logical group.

Increasing:

```text
32 -> 64 -> 128 -> 256
```

may distribute other customers more evenly.

But the hot key remains hot.

Conceptually:

```text
HOT_CUSTOMER
      |
      v
same hash key
      |
      v
one logical aggregation destination
```

Therefore:

```text
More partitions
```

is not equivalent to:

```text
Split one hot aggregation key across many final aggregation groups
```

This is one of the most important lessons in this module.

---

# 16. AQE and Aggregation Skew

Adaptive Query Execution can improve runtime behavior by using runtime statistics.

AQE can help with several forms of runtime imbalance and is particularly well known for skew handling in joins.

However:

> AQE is not a universal solution for every aggregation-key skew problem.

For a single extremely hot grouping key:

```text
GROUP BY customer_id
```

the final result still requires:

```text
HOT_CUSTOMER -> one final group
```

Therefore, simply enabling AQE should not be assumed to split one logical aggregation key into arbitrary independent final groups.

The experiment should measure what AQE actually does rather than assuming that AQE solves the problem.

---

# 17. AQE Experiment

Run the skewed workload with AQE enabled.

Record:

```text
spark.sql.adaptive.enabled
```

and relevant AQE settings.

Compare:

```text
AQE OFF
```

versus:

```text
AQE ON
```

Capture:

* Total execution time
* Number of shuffle partitions
* Task-duration distribution
* Shuffle read
* Shuffle write
* Physical plan
* Adaptive plan changes

The important question is:

> Did AQE materially reduce the straggler caused by the hot aggregation key?

The answer must come from Spark UI evidence.

---

# 18. Salting

One common technique for handling severe aggregation-key skew is:

```text
Salting
```

The idea is to artificially create additional grouping keys.

Instead of:

```text
customer_id
```

use:

```text
customer_id + salt
```

For example:

```text
HOT_CUSTOMER + 0
HOT_CUSTOMER + 1
HOT_CUSTOMER + 2
...
HOT_CUSTOMER + 15
```

This allows the workload associated with the hot key to be distributed across multiple intermediate aggregation groups.

---

# 19. Salted Aggregation — Stage 1

Example:

```scala
val salted = skewedDf.withColumn(
  "salt",
  pmod(hash(col("transaction_id")), lit(16))
)
```

Then:

```scala
val partial = salted
  .groupBy("customer_id", "salt")
  .agg(
    sum("amount").alias("partial_amount"),
    count("*").alias("partial_count")
  )
```

The intermediate grouping key becomes:

```text
(customer_id, salt)
```

instead of:

```text
customer_id
```

Therefore the hot key can be distributed across multiple salt values.

---

# 20. Salted Aggregation — Stage 2

The second stage removes the artificial salt.

```scala
val finalResult = partial
  .groupBy("customer_id")
  .agg(
    sum("partial_amount").alias("total_amount"),
    sum("partial_count").alias("transaction_count")
  )
```

Conceptually:

```text
Stage 1

HOT_CUSTOMER + 0
HOT_CUSTOMER + 1
HOT_CUSTOMER + 2
...
HOT_CUSTOMER + 15


             |
             v

Distributed intermediate aggregation


             |
             v

Stage 2

HOT_CUSTOMER
```

This creates two aggregation stages around the salted intermediate key.

---

# 21. Why Salting Works

Without salting:

```text
HOT_CUSTOMER
     |
     v
one logical grouping key
     |
     v
one heavy aggregation destination
```

With salting:

```text
HOT_CUSTOMER + 0
HOT_CUSTOMER + 1
HOT_CUSTOMER + 2
...
HOT_CUSTOMER + 15
```

The hot workload can be distributed across multiple intermediate groups.

Conceptually:

```text
                HOT_CUSTOMER
                      |
       +--------------+--------------+
       |              |              |
       v              v              v
    salt 0          salt 1         salt 2
       |              |              |
       v              v              v
    partial         partial        partial
       |              |              |
       +--------------+--------------+
                      |
                      v
               final aggregation
                      |
                      v
                HOT_CUSTOMER
```

---

# 22. Choosing the Salt Factor

Possible values:

```text
4
8
16
32
64
```

There is no universal best value.

A larger salt factor:

```text
More parallelism
```

but also:

```text
More intermediate groups
More shuffle overhead
More final aggregation work
```

Therefore the correct approach is benchmarking.

For example:

| Salt factor | Execution time | Shuffle | Straggler |
| ----------: | -------------: | ------: | --------- |
|           1 |            TBD |     TBD | TBD       |
|           4 |            TBD |     TBD | TBD       |
|           8 |            TBD |     TBD | TBD       |
|          16 |            TBD |     TBD | TBD       |
|          32 |            TBD |     TBD | TBD       |
|          64 |            TBD |     TBD | TBD       |

Do not select the largest salt factor automatically.

---

# 23. Correctness Validation

Performance optimization is incomplete without correctness validation.

Compare:

```text
Original aggregation
```

against:

```text
Salted aggregation
```

The following should match.

## Result row count

```text
COUNT(DISTINCT customer_id)
```

## Total amount

```text
SUM(total_amount)
```

## Total transaction count

```text
SUM(transaction_count)
```

## Per-key results

For a stronger validation:

```text
customer_id
total_amount
transaction_count
```

should match between the two implementations.

---

# 24. Deterministic Validation

A useful production-style validation is to calculate a deterministic checksum or comparison.

For example:

```scala
val baselineCount = baseline.count()
val saltedCount = finalResult.count()
```

Then compare aggregate totals.

The exact validation implementation can be adapted to the experiment.

The important principle is:

> Never accept a performance optimization merely because the runtime is lower. Prove that the output remains correct.

---

# 25. Benchmark Matrix

The complete experiment should eventually contain a matrix similar to:

| Test | Distribution | AQE | Salt | Shuffle Partitions | Runtime | Max/Median Task |
| ---- | ------------ | --- | ---: | -----------------: | ------: | --------------: |
| A    | Uniform      | OFF |    1 |                 32 |     TBD |             TBD |
| B    | 90% hot      | OFF |    1 |                 32 |     TBD |             TBD |
| C    | 99% hot      | OFF |    1 |                 32 |     TBD |             TBD |
| D    | 99.9% hot    | OFF |    1 |                 32 |     TBD |             TBD |
| E    | 99.9% hot    | ON  |    1 |                 32 |     TBD |             TBD |
| F    | 99.9% hot    | ON  |    4 |                 32 |     TBD |             TBD |
| G    | 99.9% hot    | ON  |    8 |                 32 |     TBD |             TBD |
| H    | 99.9% hot    | ON  |   16 |                 32 |     TBD |             TBD |
| I    | 99.9% hot    | ON  |   32 |                 32 |     TBD |             TBD |

The actual matrix can be expanded after observing the first results.

---

# 26. Important Experimental Rule

Do not change multiple variables at the same time when diagnosing the root cause.

Bad experiment:

```text
Change:
- data distribution
- AQE
- shuffle partitions
- salt factor
- caching
- executor memory

all at once
```

This makes it difficult to identify why performance changed.

Prefer:

```text
Baseline
   |
   v
Introduce skew
   |
   v
Measure
   |
   v
Change one important variable
   |
   v
Measure again
```

This produces interpretable evidence.

---

# 27. Skew vs Shuffle Partition Count

This distinction should be explicitly understood.

## Partition-count problem

Example:

```text
1 TB data
10 partitions
```

Average:

```text
100 GB / partition
```

Increasing partitions may help:

```text
10 -> 100 partitions
```

## Hot-key problem

Example:

```text
1 TB total
900 GB belongs to one grouping key
```

Increasing:

```text
32 -> 256 partitions
```

does not necessarily distribute the 900 GB hot key across independent final grouping keys.

Therefore:

```text
Partition sizing
```

and:

```text
Key skew
```

are different problems.

---

# 28. Common Misconceptions

## Misconception 1

> Data skew means there are too few partitions.

Incorrect.

Skew is about uneven distribution.

---

## Misconception 2

> Increasing shuffle partitions always fixes skew.

Incorrect.

It can improve general partition sizing but may not solve a single hot grouping key.

---

## Misconception 3

> AQE automatically solves all skew.

Incorrect.

AQE is powerful, but it is not a universal solution for every hot aggregation key.

---

## Misconception 4

> Partial aggregation eliminates skew.

Incorrect.

Partial aggregation reduces data volume but does not necessarily eliminate an extreme hot key.

---

## Misconception 5

> Salting should always be used.

Incorrect.

Salting introduces additional aggregation stages and shuffle overhead.

It should be justified by measured skew.

---

## Misconception 6

> Normal physical plan means healthy runtime.

Incorrect.

Runtime evidence from Spark UI is essential.

A physically reasonable plan can still have severe runtime skew.

---

# 29. Alternative Mitigation — Fix the Data

Before introducing salting, investigate why the hot key exists.

For example:

```text
customer_id = UNKNOWN
```

may indicate:

* Missing source data
* Broken upstream mapping
* Invalid records
* Default values
* Data-quality issues

The best solution may be to fix the source rather than optimize around the bad data.

This is an important production-engineering principle:

> Do not optimize around a data-quality defect if the defect itself can be eliminated.

---

# 30. Alternative Mitigation — Separate Exceptional Keys

If only a few keys are extremely large, it may be reasonable to process them separately.

Conceptually:

```text
Normal customers
       |
       v
Normal aggregation


Hot customers
       |
       v
Dedicated processing
```

Then combine the results.

This can be preferable when:

```text
Number of hot keys << total number of keys
```

and the hot-key processing has fundamentally different requirements.

---

# 31. Alternative Mitigation — Upstream Pre-Aggregation

If the source pipeline allows it, aggregate earlier.

For example:

```text
Raw transactions
       |
       v
Hourly aggregation
       |
       v
Daily aggregation
       |
       v
Customer aggregation
```

This reduces the amount of data reaching the final aggregation.

However, pre-aggregation does not automatically eliminate a hot final key.

It reduces the volume that the final stage must process.

---

# 32. Alternative Mitigation — Data Modeling

Sometimes the real problem is the grouping model.

If the business query repeatedly requires:

```text
GROUP BY customer_id
```

over massive historical transaction data, consider whether a precomputed or incrementally maintained aggregate is appropriate.

Possible architecture:

```text
Raw transaction events
          |
          v
Streaming / batch aggregation
          |
          v
Customer-level aggregate table
          |
          v
Fast analytical queries
```

The correct solution depends on:

* Freshness requirements
* Query frequency
* Data volume
* Update patterns
* Storage cost
* Operational complexity

---

# 33. Spark UI Evidence to Capture

For each important experiment capture:

### SQL tab

* Query execution time
* Physical plan
* Adaptive plan
* Stage boundaries

### Stage details

* Number of tasks
* Input size
* Shuffle read
* Shuffle write
* Records read
* Records written

### Task metrics

* Median duration
* Maximum duration
* Task-duration distribution
* Maximum task data size

### Executor metrics

* Memory usage
* Spill
* GC time

The screenshots are useful evidence for the final engineering analysis.

---

# 34. Evidence Table

Fill this table after running the experiment.

| Experiment | Runtime | Shuffle Read | Shuffle Write | Median Task | Max Task | Skew Ratio | Observation |
| ---------- | ------: | -----------: | ------------: | ----------: | -------: | ---------: | ----------- |
| Uniform    |     TBD |          TBD |           TBD |         TBD |      TBD |        TBD | TBD         |
| 90% skew   |     TBD |          TBD |           TBD |         TBD |      TBD |        TBD | TBD         |
| 99% skew   |     TBD |          TBD |           TBD |         TBD |      TBD |        TBD | TBD         |
| 99.9% skew |     TBD |          TBD |           TBD |         TBD |      TBD |        TBD | TBD         |
| AQE        |     TBD |          TBD |           TBD |         TBD |      TBD |        TBD | TBD         |
| Salt 4     |     TBD |          TBD |           TBD |         TBD |      TBD |        TBD | TBD         |
| Salt 8     |     TBD |          TBD |           TBD |         TBD |      TBD |        TBD | TBD         |
| Salt 16    |     TBD |          TBD |           TBD |         TBD |      TBD |        TBD | TBD         |
| Salt 32    |     TBD |          TBD |           TBD |         TBD |      TBD |        TBD | TBD         |

---

# 35. Production Diagnostic Workflow

A senior Spark engineer should approach aggregation skew systematically.

```text
                 Job is slow
                     |
                     v
              Inspect Spark UI
                     |
                     v
          Are tasks imbalanced?
               /           \
             No             Yes
             |               |
             v               v
       Investigate      Inspect data
       other causes      distribution
                             |
                             v
                     Is there a hot key?
                        /         \
                      No           Yes
                      |             |
                      v             v
              Investigate      Quantify skew
              other causes          |
                                    v
                           Check partition sizing
                                    |
                                    v
                              Test AQE behavior
                                    |
                                    v
                         Evaluate mitigation
                           /              \
                      Data fix          Salting
                           \              /
                            v            v
                         Benchmark both
                                |
                                v
                         Validate correctness
                                |
                                v
                          Production decision
```

---

# 36. Senior Engineering Decision Framework

The correct engineering process is:

```text
1. Measure
2. Diagnose
3. Quantify
4. Change
5. Benchmark
6. Validate correctness
7. Compare trade-offs
8. Adopt
9. Monitor
```

Do not start with:

```text
"Let's increase shuffle partitions."
```

or:

```text
"Let's enable AQE."
```

or:

```text
"Let's add salting."
```

Start with evidence.

---

# 37. Interview-Level Explanation

A strong senior-level explanation would be:

> Aggregation skew occurs when a small number of grouping keys contain a disproportionate amount of data. During the shuffle required by a `GROUP BY`, rows are partitioned according to the grouping key. A hot key can therefore create a heavily loaded partition and a long-running straggler task. Increasing `spark.sql.shuffle.partitions` may improve general partition sizing but does not necessarily split one hot grouping key across multiple final groups. I would first confirm the skew using Spark UI task-duration and shuffle metrics, then evaluate AQE behavior and consider mitigation such as salting, separate processing for exceptional keys, upstream data-quality fixes, or pre-aggregation. Any optimization would be benchmarked and validated for correctness before production adoption.

This demonstrates understanding of:

* Logical plans
* Physical plans
* Shuffle
* Hash partitioning
* Aggregation
* AQE
* Runtime diagnostics
* Performance engineering
* Correctness
* Production trade-offs

---

# 38. Key Engineering Insight

The most important concept in this module is:

```text
More partitions != automatic hot-key parallelism
```

A partition-count problem can often be improved by increasing:

```text
spark.sql.shuffle.partitions
```

A hot-key problem may require changing the aggregation strategy itself.

For example:

```text
Normal:

GROUP BY customer_id
```

versus:

```text
Skew mitigation:

GROUP BY customer_id, salt
        |
        v
GROUP BY customer_id
```

The second approach changes the execution strategy.

---

# 39. Learning Outcomes

After completing this module, you should be able to explain:

* What data skew is
* What aggregation-key skew is
* Why skew creates straggler tasks
* How hash partitioning affects aggregation
* Why a hot key can dominate one partition
* Why increasing shuffle partitions may not solve hot-key skew
* How partial aggregation reduces shuffle volume
* What AQE can and cannot solve
* How salting works
* Why salting requires a second aggregation
* How to select a salt factor experimentally
* How to validate correctness after salting
* How to diagnose skew using Spark UI
* How to distinguish skew from partition-sizing problems
* When fixing the source data is better than adding Spark optimizations
* How to make a production performance decision based on evidence

---

# 40. Final Takeaways

### 1. Data skew is a data-distribution problem

A few keys can dominate a workload.

### 2. Aggregation requires grouping-key locality

Rows belonging to the same logical key must eventually be combined.

### 3. Partial aggregation reduces volume

But it does not necessarily eliminate hot-key skew.

### 4. More shuffle partitions are not a universal solution

They do not automatically split one hot aggregation key into many final groups.

### 5. AQE is useful but not magic

Always verify its actual runtime behavior.

### 6. Salting changes the aggregation strategy

It creates multiple intermediate groups for a hot key and then performs a final aggregation.

### 7. Salting has a cost

It introduces extra computation, shuffle, and aggregation work.

### 8. Spark UI is essential

Use task and shuffle metrics to prove skew.

### 9. Correctness comes before performance

A faster result that is incorrect is not an optimization.

### 10. Fix the root cause when possible

If skew comes from invalid or missing source data, fixing the data may be better than compensating in Spark.

---

# 41. Completion Checklist

Before marking Module 1.7.9 complete:

* [ ] Run uniform aggregation
* [ ] Run 90% skew aggregation
* [ ] Run 99% skew aggregation
* [ ] Run 99.9% skew aggregation
* [ ] Capture physical plans
* [ ] Capture Spark UI SQL details
* [ ] Capture task-duration distribution
* [ ] Capture shuffle read/write
* [ ] Calculate max/median task-duration ratio
* [ ] Test AQE
* [ ] Test multiple salt factors
* [ ] Compare runtime
* [ ] Compare shuffle overhead
* [ ] Validate result row counts
* [ ] Validate total amount
* [ ] Validate total transaction count
* [ ] Compare per-key results
* [ ] Identify the best-performing configuration
* [ ] Document trade-offs
* [ ] Record production recommendation

---

# 42. Module Relationship

The aggregation learning path is:

```text
1.7.1 — Baseline Aggregation
        |
        v
1.7.2 — Hash Aggregation
        |
        v
1.7.3 — Partial Aggregation
        |
        v
1.7.4 — Shuffle / Exchange
        |
        v
1.7.5 — Aggregation Performance Investigation
        |
        v
1.7.6 — Aggregation Memory / Spill Behavior
        |
        v
1.7.7 — Aggregation and AQE
        |
        v
1.7.8 — Aggregation Partitioning
        |
        v
1.7.9 — Data Skew in Aggregation
```

This module connects the earlier concepts:

```text
HashAggregate
     +
Partial Aggregation
     +
Exchange
     +
Partitioning
     +
AQE
     |
     v
Data Skew
     |
     v
Production Optimization
```

---

# 43. Final Senior-Level Principle

The objective of Spark performance engineering is not:

```text
Make Spark run faster
```

The objective is:

```text
Understand the workload
        +
Understand the physical execution
        +
Measure the bottleneck
        +
Apply the smallest effective optimization
        +
Validate correctness
        +
Measure the trade-off
        +
Make an evidence-based production decision
```

That is the difference between knowing Spark APIs and understanding Spark as a distributed execution engine.
