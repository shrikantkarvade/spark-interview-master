Next is **Module 1.7.3 — Partial Aggregation**. This one is especially important because it connects the `HashAggregate` internals from 1.7.2 to the actual **shuffle reduction behavior**.

# Module 1.7.3 — Partial Aggregation

## Objective

Understand how Apache Spark performs **partial aggregation before a shuffle**, why partial aggregation can dramatically reduce shuffle volume, and how that benefit changes as aggregation cardinality increases.

This experiment focuses on the relationship between:

```text
Input Rows
    ↓
Partial Aggregation
    ↓
Shuffle Volume
    ↓
Final Aggregation
```

The goal is to move beyond simply observing `HashAggregate` in the physical plan and quantify **how much work Spark eliminates before the network/shuffle boundary**.

---

# 1. Use Case

Consider a large financial transaction platform processing millions or billions of transactions.

A common requirement is to calculate:

```text
Total transaction amount per customer
```

For example:

```text
Customer 1001 → ₹15,420,000
Customer 1002 → ₹ 8,720,000
Customer 1003 → ₹23,180,000
```

A naïve distributed implementation could potentially shuffle every transaction:

```text
10M transactions
       ↓
    Shuffle
       ↓
Final aggregation
```

That would be expensive.

Spark instead attempts to aggregate records locally before the shuffle:

```text
10M transactions
       ↓
Partial aggregation
       ↓
200K partial records
       ↓
Shuffle
       ↓
100K final customer groups
```

The difference between these two approaches can be enormous.

---

# 2. Engineering Question

The central question is:

> **How effectively can Spark reduce aggregation data before the shuffle?**

We specifically want to understand:

1. How many rows enter the aggregation?
2. How many rows remain after partial aggregation?
3. How many records are actually shuffled?
4. How much shuffle data is generated?
5. How does grouping-key cardinality affect partial aggregation?
6. How does aggregation memory change?
7. Does the physical plan change when cardinality changes?
8. Can the same physical plan have radically different performance characteristics?

---

# 3. Hypothesis

For a low-cardinality workload:

```text
Many input rows
        ↓
Few distinct groups
```

there should be significant opportunity for partial aggregation.

Therefore we expect:

```text
Input rows >> Partial rows
```

For a higher-cardinality workload:

```text
Many input rows
        ↓
Many distinct groups
```

partial aggregation becomes less effective.

Therefore:

```text
Input rows ≈ Partial rows
```

The expected relationship is:

```text
Lower cardinality
        ↓
More duplicate grouping keys
        ↓
More local aggregation
        ↓
Fewer shuffle records
        ↓
Less shuffle data
```

Conversely:

```text
Higher cardinality
        ↓
Fewer duplicate grouping keys
        ↓
Less local aggregation
        ↓
More shuffle records
        ↓
More memory pressure
```

---

# 4. Workload

The control workload uses:

| Parameter          |         Value |
| ------------------ | ------------: |
| Transactions       |    10,000,000 |
| Customers          |       100,000 |
| Shuffle partitions |            20 |
| Aggregation        | `SUM(amount)` |
| Grouping key       | `customer_id` |
| AQE                |       Enabled |
| Execution          |         Local |

The average number of transactions per customer is:

```text
10,000,000 / 100,000
= 100 transactions/customer
```

This provides substantial opportunity for local aggregation.

---

# 5. Deterministic Dataset

The dataset is generated using:

```scala
spark.range(0, transactionCount)
  .select(
    col("id").alias("transaction_id"),
    (col("id") % customerCount).alias("customer_id"),
    (col("id") % 1000).alias("amount")
  )
```

The modulo operation creates a deterministic and evenly distributed customer-key pattern.

For the 100K-cardinality workload:

```text
10M rows
100K customers
```

there are approximately:

```text
100 rows/customer
```

---

# 6. Aggregation

The aggregation is:

```scala
val aggregation =
  transactions
    .groupBy("customer_id")
    .agg(
      sum("amount").alias("total_amount")
    )
```

The logical operation is straightforward:

```text
GROUP BY customer_id
SUM(amount)
```

The important part of this experiment is not the logical expression.

It is what happens before the Exchange.

---

# 7. Execution Command

Compile:

```bash
./gradlew compileScala
```

Run the control experiment:

```bash
./gradlew runPartialAggregationExperiment --args="10000000 100000 20 --ui-pause=300"
```

The parameters are:

```text
10000000 → 10M transactions
100000   → 100K customers
20       → 20 configured shuffle partitions
```

The Spark UI remains available for 300 seconds.

---

# 8. Physical Execution Plan

The physical plan is structurally:

```text
AdaptiveSparkPlan
+- HashAggregate
   +- Exchange
      +- HashAggregate
         +- Project
            +- Range
```

The executed plan contains:

```text
AdaptiveSparkPlan
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

The important operator is:

```text
HashAggregate [partial_sum]
```

before:

```text
Exchange
```

This is the physical evidence that Spark is performing partial aggregation before the shuffle.

---

# 9. What Partial Aggregation Means

Suppose one input partition contains:

```text
customer_id | amount
------------|-------
100         | 50
100         | 30
100         | 20
200         | 70
200         | 10
```

Without partial aggregation, all five rows could potentially participate in the shuffle.

With partial aggregation:

```text
customer_id | partial_sum
------------|------------
100         | 100
200         | 80
```

Only two records need to cross the shuffle boundary.

Therefore:

```text
5 input rows
    ↓
2 partial rows
```

The same principle applies at millions or billions of rows.

---

# 10. Control Experiment — 100K Keys

The first experiment uses:

```text
10M transactions
100K customers
```

The observed results were:

| Metric               |       Result |
| -------------------- | -----------: |
| Input rows           |   10,000,000 |
| Partial rows         |      200,000 |
| Final rows           |      100,000 |
| Shuffle records      |      200,000 |
| Shuffle bytes        |  1,963.8 KiB |
| AQE final partitions |            1 |
| AQE partition data   | ~2,044.2 KiB |
| Spill                |          0 B |
| Sort fallback        |            0 |

The key observation is:

```text
10,000,000
       ↓
   200,000
```

---

# 11. Partial Aggregation Reduction

The reduction is:

```text
1 - (200,000 / 10,000,000)
```

which equals:

```text
0.98
```

or:

# 98%

Therefore, approximately **98% of the input records were eliminated before the shuffle**.

This is one of the strongest pieces of evidence in the experiment.

---

# 12. Why Partial Output Is 200K

At first glance, we might expect:

```text
100K customers
```

to produce:

```text
100K partial rows
```

But the source `Range` has:

```text
splits = Some(2)
```

Therefore the input is processed through two source partitions.

Each source partition can independently produce approximately:

```text
100K customer groups
```

giving:

```text
100K × 2
= 200K partial groups
```

The important rule is:

> **Partial aggregation is local to each upstream partition.**

Therefore:

```text
Partial rows ≠ necessarily global distinct keys
```

---

# 13. Why This Matters

This distinction becomes very important when estimating shuffle volume.

Suppose:

```text
100M input rows
10M distinct customers
```

A simplistic calculation might assume:

```text
10M rows shuffled
```

But actual partial aggregation output depends on:

* Number of input partitions
* Key distribution
* Number of unique keys within each partition
* Duplicate frequency
* Partitioning strategy

Therefore, the correct engineering approach is:

> **Measure partial aggregation output rather than estimating it solely from global cardinality.**

---

# 14. Shuffle Metrics

The control run generated approximately:

```text
200,000 shuffle records
```

and:

```text
1,963.8 KiB
```

of shuffle data.

This means the shuffle carried the partial representation rather than the full 10M-row input.

Conceptually:

```text
10M input records
       ↓
Partial aggregation
       ↓
200K records
       ↓
~1.92 MiB shuffle
```

---

# 15. Hash Aggregation Metrics

The Spark UI reported:

```text
Partial hash probes ≈ 1.1
Final hash probes   ≈ 1.4
```

This indicates relatively efficient hash-table behavior for the control workload.

The aggregation build time was approximately:

```text
895 ms
```

with peak aggregation memory around:

```text
132 MiB total
```

and:

```text
66 MiB maximum per task
```

No spilling occurred.

---

# 16. Spill Behavior

Observed:

```text
Memory spill = 0 B
Disk spill   = 0 B
```

This is important because partial aggregation trades:

```text
Memory
```

for:

```text
Reduced shuffle volume
```

The local hash aggregation needs memory to maintain:

```text
customer_id → partial SUM
```

But the workload fits comfortably within the available memory.

---

# 17. AQE Behavior

The configured shuffle partition count was:

```text
20
```

However, AQE reduced the final read to:

```text
1 partition
```

The resulting shuffle data was only around:

```text
2 MiB
```

so AQE could safely coalesce the small shuffle partitions.

Therefore:

```text
Configured shuffle partitions = 20
AQE final partitions          = 1
```

This is another example of why configured Spark settings should not be confused with final runtime behavior.

---

# 18. Second Experiment — 1M Keys

To demonstrate the effect of cardinality, the experiment was also executed with:

```bash
./gradlew runPartialAggregationExperiment --args="10000000 1000000 --ui-pause=300"
```

This changes the workload to:

```text
10M transactions
1M customers
```

The average number of transactions per customer becomes:

```text
10,000,000 / 1,000,000
= 10 transactions/customer
```

Compared with the control:

```text
100K keys → 100 rows/key
1M keys   → 10 rows/key
```

The amount of duplication available to partial aggregation is therefore significantly lower.

---

# 19. 1M-Key Results

Observed:

| Metric               | 100K Keys |  1M Keys |
| -------------------- | --------: | -------: |
| Input rows           |       10M |      10M |
| Average rows/key     |       100 |       10 |
| Partial rows         |      200K |       2M |
| Final rows           |      100K |       1M |
| Shuffle records      |      200K |       2M |
| Shuffle bytes        |  1.92 MiB | 17.4 MiB |
| Shuffle reduction    |       98% |      80% |
| AQE final partitions |         1 |        2 |
| Spill                |         0 |        0 |
| Partial hash probes  |       1.1 |      1.4 |
| Final hash probes    |       1.4 |      1.5 |

The most important observation is:

```text
100K keys → 200K partial rows
1M keys   → 2M partial rows
```

A 10× increase in grouping cardinality resulted in a 10× increase in partial rows.

---

# 20. Reduction Falls from 98% to 80%

For 100K keys:

```text
10M → 200K
```

Reduction:

```text
98%
```

For 1M keys:

```text
10M → 2M
```

Reduction:

```text
80%
```

This demonstrates a critical principle:

> **Partial aggregation becomes less effective as grouping cardinality approaches input cardinality.**

The physical plan is essentially unchanged.

The data characteristics changed.

---

# 21. Shuffle Growth

Shuffle data increased from:

```text
1.92 MiB
```

to:

```text
17.4 MiB
```

This is approximately a:

```text
9×
```

increase in shuffle bytes.

The number of shuffle records increased exactly:

```text
200K → 2M
```

or:

```text
10×
```

---

# 22. Memory Growth

The higher-cardinality workload also increased memory requirements.

For the 100K-key case:

```text
Peak aggregation memory ≈ 132 MiB
Maximum task ≈ 66 MiB
```

For the 1M-key case:

```text
Partial aggregation peak ≈ 192 MiB
Maximum task ≈ 96 MiB
```

The final aggregation also consumed approximately:

```text
160 MiB total
80 MiB maximum task
```

The important observation is that increasing cardinality increases the amount of aggregation state that Spark must maintain.

---

# 23. Aggregation Build Time

The 100K-key workload showed approximately:

```text
895 ms
```

of partial aggregation build time.

At 1M keys:

```text
~1.9 s
```

was observed for partial aggregation.

This demonstrates another consequence of cardinality:

```text
More groups
    ↓
Larger hash state
    ↓
More hash operations
    ↓
More memory management
    ↓
Higher aggregation cost
```

---

# 24. Whole-Stage Codegen

The 100K-key run showed:

```text
WholeStageCodegen (1) ≈ 1.4 s
WholeStageCodegen (2) ≈ 292 ms
```

The 1M-key run showed:

```text
WholeStageCodegen (1) ≈ 2.6 s
WholeStageCodegen (2) ≈ 1.1 s
```

Again, the physical operator family did not change.

The workload became more expensive because the aggregation state became larger.

---

# 25. Physical Plan Stability

One of the most valuable observations is that the physical plan remains structurally the same.

For both cardinalities:

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

Yet runtime characteristics changed substantially.

This proves an important engineering principle:

> **A physical plan describes execution strategy, but runtime data characteristics determine how expensive that strategy becomes.**

Looking only at the plan is therefore insufficient.

---

# 26. Comparison

The two experiments can be summarized as:

```text
100K keys

10M input
   ↓
200K partial
   ↓
1.92 MiB shuffle
   ↓
100K final
```

versus:

```text
1M keys

10M input
   ↓
2M partial
   ↓
17.4 MiB shuffle
   ↓
1M final
```

The SQL is identical.

The execution strategy is essentially identical.

The cardinality is different.

The resulting workload is substantially different.

---

# 27. Partial Aggregation Efficiency

A useful metric is:

```text
Partial Aggregation Reduction
=
1 - (Partial Rows / Input Rows)
```

For 100K keys:

```text
1 - (200K / 10M)
= 98%
```

For 1M keys:

```text
1 - (2M / 10M)
= 80%
```

This gives us a practical measure of how effectively the aggregation reduces data before the shuffle.

---

# 28. Cardinality Relationship

The experiments demonstrate:

```text
                 Higher cardinality
                        ↓
             Fewer duplicate keys
                        ↓
            Less local aggregation
                        ↓
               More partial rows
                        ↓
               More shuffle data
                        ↓
             More aggregation state
                        ↓
               Higher memory usage
```

This relationship becomes even more dramatic at very high cardinalities.

---

# 29. Why Partial Aggregation Is Not Always a Huge Win

Partial aggregation is most effective when multiple input records share the same grouping key within an upstream partition.

For example:

```text
100 rows
   ↓
1 grouping key
```

can become:

```text
1 partial row
```

That's a:

```text
99% reduction
```

But:

```text
100 rows
   ↓
100 unique grouping keys
```

remains:

```text
100 partial rows
```

There is no record reduction.

Therefore:

> **Partial aggregation effectiveness depends on duplicate grouping keys within each input partition, not merely on the global number of groups.**

---

# 30. Local vs Global Cardinality

This is an important distributed-systems distinction.

Global cardinality tells us:

```text
How many unique groups exist across the entire dataset?
```

Local cardinality tells us:

```text
How many unique groups exist within each input partition?
```

Partial aggregation operates on the second.

Therefore:

```text
Global cardinality
        ≠
Local aggregation effectiveness
```

Two datasets with the same global cardinality can have very different partial aggregation behavior if their keys are distributed differently.

---

# 31. Example

Consider:

```text
10M rows
1M global keys
```

Dataset A:

```text
Keys are highly repeated within each partition
```

could achieve strong local reduction.

Dataset B:

```text
Each partition contains mostly unique keys
```

could produce almost one partial record per input record.

Both datasets have:

```text
1M global keys
```

but their shuffle volumes can be very different.

This is why production benchmarking must consider actual data distribution.

---

# 32. What the Spark UI Proves

The Spark UI provides evidence that:

* Partial aggregation occurred.
* 10M input rows were processed.
* Partial output changed with cardinality.
* Shuffle records changed with cardinality.
* Shuffle bytes increased with cardinality.
* Aggregation memory increased with cardinality.
* Hash probe counts increased modestly.
* No spill occurred for these workloads.
* AQE adapted the final shuffle read.
* The physical operator remained HashAggregate.

---

# 33. What the Spark UI Does Not Prove

The local benchmark does not prove:

* Production cluster throughput
* Production network latency
* Production executor memory behavior
* Production spill thresholds
* Optimal shuffle partition count
* SLA performance
* Behavior under real-world skew

These experiments are designed to understand **mechanics and relationships**, not to establish production capacity.

---

# 34. Key Engineering Observation

The most important result is:

```text
Same SQL
Same physical strategy
Different cardinality
Different execution cost
```

This can be represented as:

```text
GROUP BY + SUM
       ↓
HashAggregate
       ↓
Same physical plan
       ↓
--------------------------------
| 100K keys | 1M keys          |
--------------------------------
| 200K rows | 2M rows          |
| 1.92 MiB  | 17.4 MiB         |
| 98% red.  | 80% red.          |
| 132 MiB   | 192 MiB           |
--------------------------------
```

The data, not the syntax, is driving the difference.

---

# 35. Engineering Decision

For production aggregation workloads:

> **Measure partial aggregation effectiveness before attempting shuffle or cluster-level tuning.**

Useful production metrics include:

```text
Input rows
Partial output rows
Shuffle records
Shuffle bytes
Distinct grouping keys
Rows per key
Aggregation memory
Spill
```

A useful diagnostic ratio is:

```text
Partial rows / Input rows
```

Lower is generally better for shuffle reduction.

For example:

```text
200K / 10M = 2%
```

means only 2% of input records survived into the partial output.

Whereas:

```text
10M / 10M = 100%
```

means partial aggregation eliminated no records.

---

# 36. Production Diagnostic Framework

When an aggregation is unexpectedly slow, investigate in this order:

```text
1. Input volume
       ↓
2. Grouping-key cardinality
       ↓
3. Local duplicate rate
       ↓
4. Partial aggregation output
       ↓
5. Shuffle records
       ↓
6. Shuffle bytes
       ↓
7. Aggregation memory
       ↓
8. Spill
       ↓
9. AQE behavior
       ↓
10. Task distribution
```

This is more useful than immediately changing:

```text
spark.sql.shuffle.partitions
```

or adding random repartitions.

---

# 37. Experiment Matrix

The two completed runs provide the following control matrix:

| Keys | Avg rows/key | Partial rows | Final rows | Shuffle records |  Shuffle | Reduction | Partial Peak Memory |
| ---: | -----------: | -----------: | ---------: | --------------: | -------: | --------: | ------------------: |
| 100K |          100 |         200K |       100K |            200K | 1.92 MiB |       98% |             132 MiB |
|   1M |           10 |           2M |         1M |              2M | 17.4 MiB |       80% |             192 MiB |

This matrix becomes the foundation for the upcoming cardinality experiment.

---

# 38. Execution Model

The final execution can be represented as:

```text
                    Input
                  10M rows
                      │
                      ▼
              ┌───────────────┐
              │ Range / Input │
              └───────┬───────┘
                      │
                      ▼
          ┌────────────────────────┐
          │ Partial HashAggregate  │
          │ customer_id → SUM      │
          └────────────┬───────────┘
                       │
             100K case: 200K rows
             1M case:   2M rows
                       │
                       ▼
                ┌────────────┐
                │  Exchange  │
                └─────┬──────┘
                      │
                      ▼
              ┌───────────────┐
              │ AQE Shuffle   │
              │ Read          │
              └───────┬───────┘
                      │
                      ▼
          ┌────────────────────────┐
          │ Final HashAggregate    │
          │ merge partial results  │
          └────────────┬───────────┘
                       │
                       ▼
              Final customer groups
```

---

# 39. Relationship to Hash Aggregation

Module 1.7.2 established:

```text
Spark uses HashAggregate
```

Module 1.7.3 adds the more important performance dimension:

```text
HashAggregate
     ↓
Partial aggregation
     ↓
Shuffle reduction
```

Therefore the learning progression is:

```text
1.7.1
Baseline Aggregation
       ↓
1.7.2
Hash Aggregation
       ↓
1.7.3
Partial Aggregation
       ↓
1.7.4
Aggregation Cardinality
```

The experiments are deliberately connected.

---

# 40. Relationship to Shuffle

Partial aggregation directly influences shuffle.

For the 100K-key workload:

```text
Input:
10M rows

Partial:
200K rows

Shuffle:
~1.92 MiB
```

For 1M keys:

```text
Input:
10M rows

Partial:
2M rows

Shuffle:
~17.4 MiB
```

Therefore:

```text
Partial aggregation
        ↓
Shuffle reduction
```

is one of the central performance mechanisms in distributed aggregation.

---

# 41. Why You Should Not Tune Blindly

Suppose a production aggregation is slow.

An engineer might immediately change:

```text
spark.sql.shuffle.partitions
```

But if the real problem is:

```text
10M rows
10M distinct keys
```

then changing the shuffle partition count does not magically create aggregation opportunities.

The underlying problem is high cardinality.

Likewise, if the real problem is skew:

```text
One key contains 40% of all records
```

then simply increasing partitions may not solve the hot-key problem.

Therefore:

> **First understand the data shape. Then tune the physical execution.**

---

# 42. Limitations

This experiment has several limitations.

### Local execution

The benchmark runs on a local Windows environment.

Therefore it does not model:

* multi-executor networking
* distributed scheduling
* executor-to-executor network transfer
* production disk behavior
* production memory pressure

### Deterministic distribution

The modulo-based customer generation is intentionally uniform.

Real workloads may have:

* hot customers
* skewed keys
* null keys
* bursty transaction patterns
* variable record sizes

### Limited cardinality matrix

This experiment compares the 100K and 1M control cases.

A broader cardinality sweep is performed in Module 1.7.4.

---

# 43. Reproducibility

Environment:

```text
Apache Spark 3.5.1
Scala 2.12.18
Java 17.0.20.101
Gradle 9.6.0
```

Control:

```bash
./gradlew compileScala
./gradlew runPartialAggregationExperiment --args="10000000 100000 20 --ui-pause=300"
```

Higher-cardinality run:

```bash
./gradlew runPartialAggregationExperiment --args="10000000 1000000 --ui-pause=300"
```

The input generation is deterministic, allowing the results to be reproduced.

---

# 44. Final Findings

The experiment demonstrates several important Spark engineering principles.

### 1. Partial aggregation happens before the shuffle

```text
Partial HashAggregate
        ↓
Exchange
```

is visible directly in the physical plan.

### 2. Partial aggregation can dramatically reduce shuffle

The 100K-key workload achieved:

```text
98% record reduction
```

before the shuffle.

### 3. Cardinality controls aggregation effectiveness

Increasing keys from:

```text
100K → 1M
```

reduced the observed shuffle-record reduction from:

```text
98% → 80%
```

### 4. Higher cardinality increases memory requirements

Peak partial aggregation memory increased from approximately:

```text
132 MiB → 192 MiB
```

### 5. Higher cardinality increases shuffle

Shuffle grew from:

```text
1.92 MiB → 17.4 MiB
```

### 6. The physical plan can remain unchanged

The same:

```text
HashAggregate → Exchange → HashAggregate
```

strategy can have substantially different runtime behavior.

### 7. AQE adapts the final execution

The 100K-key workload was reduced to:

```text
1 AQE read partition
```

while the 1M-key workload used:

```text
2 AQE read partitions
```

---

# 45. Final Engineering Principle

> **Partial aggregation is one of Spark's most important mechanisms for reducing distributed aggregation cost, but its effectiveness depends on the amount of key duplication available within each upstream partition.**

The key relationship is:

```text
Grouping Cardinality
        ↓
Local Duplicate Rate
        ↓
Partial Aggregation Effectiveness
        ↓
Shuffle Records
        ↓
Shuffle Bytes
        ↓
Memory / CPU
        ↓
Overall Runtime
```

Therefore:

> **Do not assume that `GROUP BY` is cheap simply because Spark performs partial aggregation. Measure how much data the partial aggregation actually removes.**

The next experiment will take this one step further by systematically varying aggregation cardinality and quantifying exactly where partial aggregation begins to lose its benefit.

**Next: Module 1.7.4 — Aggregation Cardinality.**
