Next is **Module 1.7.4 — Aggregation Cardinality**. This is the most data-driven experiment in the sequence so far because we sweep **100K → 500K → 1M → 5M → 10M keys** while keeping the input fixed at 10M rows.

# Module 1.7.4 — Aggregation Cardinality

## Objective

Understand how **aggregation cardinality** affects Apache Spark aggregation performance.

This experiment keeps the input volume and execution configuration constant while changing only the number of grouping keys.

The experiment measures:

* Partial aggregation output
* Final aggregation output
* Shuffle records
* Shuffle bytes
* Shuffle reduction
* Hash probe behavior
* Aggregation memory
* Aggregation build time
* Whole-stage code generation
* AQE partition coalescing
* Spill behavior
* Physical plan stability

The central engineering question is:

> **How does increasing the number of distinct grouping keys change the cost and effectiveness of Spark's hash aggregation?**

---

# 1. Use Case

Consider a large financial transaction platform processing:

```text
10 million transactions
```

and calculating:

```text
SUM(amount)
GROUP BY customer_id
```

The same business logic can behave very differently depending on the number of customers.

For example:

```text
Scenario A
10M transactions
100K customers

Scenario B
10M transactions
1M customers

Scenario C
10M transactions
10M customers
```

The SQL is identical.

The input row count is identical.

The Spark aggregation strategy can remain identical.

Yet the execution cost can change dramatically.

This experiment isolates that variable.

---

# 2. Engineering Question

The experiment asks:

> **What happens to Spark aggregation as the number of grouping keys approaches the number of input rows?**

Specifically:

1. How does partial aggregation output change?
2. How does shuffle volume change?
3. When does partial aggregation stop providing meaningful record reduction?
4. How does aggregation memory change?
5. How does aggregation build time change?
6. Does the physical plan change?
7. Does AQE compensate for increasing cardinality?
8. At what point does aggregation become effectively equivalent to shuffling the input?

---

# 3. Hypothesis

The expected relationship is:

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
Larger aggregation state
        ↓
Higher memory consumption
        ↓
Higher CPU cost
        ↓
Longer execution
```

At very low cardinality:

```text
10M rows
100K keys
```

many rows share the same key.

Partial aggregation should therefore be highly effective.

At very high cardinality:

```text
10M rows
10M keys
```

each row effectively represents its own group.

There is then little or no opportunity to reduce records before the shuffle.

---

# 4. Experimental Design

The input is fixed:

```text
10,000,000 rows
```

The configured shuffle partitions are fixed:

```text
20
```

Only the number of grouping keys changes.

The tested cardinalities are:

```scala id="y0i8m7"
val cardinalities =
  Seq(
    100000L,
    500000L,
    1000000L,
    5000000L,
    10000000L
  )
```

Therefore:

```text
100K
500K
1M
5M
10M
```

keys are tested.

---

# 5. Experimental Matrix

| Parameter          |                   Value |
| ------------------ | ----------------------: |
| Input rows         |                     10M |
| Cardinalities      | 100K, 500K, 1M, 5M, 10M |
| Shuffle partitions |                      20 |
| Aggregation        |           `SUM(amount)` |
| Grouping key       |           `customer_id` |
| AQE                |                 Enabled |
| Execution          |                   Local |

The experiment creates a fresh aggregation for each cardinality.

This is important because the physical plan must be generated **after** the relevant configuration and workload have been established.

---

# 6. Dataset Generation

The deterministic dataset is generated using:

```scala id="6qltna"
spark.range(0, transactionCount)
  .select(
    col("id").alias("transaction_id"),
    (col("id") % customerCount).alias("customer_id"),
    (col("id") % 1000).alias("amount")
  )
```

The modulo operation produces a predictable relationship between:

```text
transaction_id
customer_id
amount
```

This allows us to change cardinality while keeping the input volume constant.

---

# 7. Aggregation

The aggregation remains unchanged:

```scala id="uw3u0x"
transactions
  .groupBy("customer_id")
  .agg(
    sum("amount").alias("total_amount")
  )
```

This is intentional.

We are **not** changing the SQL/DataFrame operation.

We are changing the data characteristics.

That allows us to isolate cardinality as the primary experimental variable.

---

# 8. Execution Command

Compile:

```bash id="wqce3n"
./gradlew compileScala
```

Run:

```bash id="sl9f3p"
./gradlew runAggregationCardinalityExperiment --args="10000000 20 --ui-pause=300"
```

Arguments:

```text id="efg4xr"
10000000 → total input rows
20       → configured shuffle partitions
```

The experiment then executes the five cardinality cases:

```text
100K
500K
1M
5M
10M
```

---

# 9. Physical Plan

The physical plan remains structurally consistent throughout the experiment.

Representative final plan:

```text id="h3kj5r"
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

The key execution path is:

```text id="wq1a9s"
Range
  ↓
Project
  ↓
Partial HashAggregate
  ↓
Exchange
  ↓
AQE Shuffle Read
  ↓
Final HashAggregate
```

The important finding is that the physical strategy does not need to change for the workload to become much more expensive.

---

# 10. Why This Experiment Is Important

A common Spark troubleshooting mistake is:

> "The physical plan looks correct, therefore the query should be fast."

That assumption is incorrect.

A plan can be structurally correct while the data flowing through it becomes increasingly expensive.

This experiment demonstrates exactly that.

The physical plan remains essentially:

```text
HashAggregate
Exchange
HashAggregate
```

while:

```text
10M rows
100K keys
```

and:

```text
10M rows
10M keys
```

have radically different execution characteristics.

---

# 11. Query 0 — 100K Keys

The first workload uses:

```text
10M input rows
100K grouping keys
```

Average rows per key:

```text
10,000,000 / 100,000
= 100
```

Observed:

| Metric                    |      Result |
| ------------------------- | ----------: |
| Input rows                |         10M |
| Partial rows              |        200K |
| Final rows                |        100K |
| Shuffle records           |        200K |
| Shuffle bytes             | 1,963.8 KiB |
| Reduction                 |         98% |
| AQE final partitions      |           1 |
| AQE partition data        |    ~2.0 MiB |
| Spill                     |         0 B |
| Sort fallback             |           0 |
| Partial hash probes       |        ~1.1 |
| Final hash probes         |        ~1.4 |
| Partial aggregation build |      ~1.2 s |
| Partial peak memory       |    ~132 MiB |
| Final aggregation build   |     ~163 ms |
| Final peak memory         |     ~68 MiB |

This is the most aggregation-friendly workload in the experiment.

---

# 12. Why 100K Keys Performs Well

There are:

```text
100 transactions/customer on average
```

so each input partition contains many repeated grouping keys.

That allows the partial aggregation to combine records locally.

The result:

```text
10M input rows
       ↓
200K partial rows
```

represents:

```text
98% record reduction
```

before the shuffle.

This is a very strong partial aggregation benefit.

---

# 13. Query 1 — 500K Keys

The second workload uses:

```text
10M input rows
500K grouping keys
```

Average rows per key:

```text
10,000,000 / 500,000
= 20
```

Observed:

| Metric                    |   Result |
| ------------------------- | -------: |
| Input rows                |      10M |
| Partial rows              |       1M |
| Final rows                |     500K |
| Shuffle records           |       1M |
| Shuffle bytes             |  8.7 MiB |
| Reduction                 |      90% |
| AQE final partitions      |        2 |
| AQE partition data        | ~9.2 MiB |
| Spill                     |      0 B |
| Sort fallback             |        0 |
| Partial hash probes       |     ~1.3 |
| Final hash probes         |     ~1.5 |
| Partial aggregation build |   ~1.8 s |
| Partial peak memory       | ~160 MiB |
| Final aggregation build   |  ~439 ms |
| Final peak memory         | ~144 MiB |

Compared with 100K keys:

```text
100K → 500K
```

is a 5× increase in global cardinality.

Partial output increased:

```text
200K → 1M
```

and shuffle increased:

```text
1.92 MiB → 8.7 MiB
```

---

# 14. Query 2 — 1M Keys

The third workload uses:

```text
10M input rows
1M grouping keys
```

Average rows per key:

```text
10,000,000 / 1,000,000
= 10
```

Observed:

| Metric                    |    Result |
| ------------------------- | --------: |
| Input rows                |       10M |
| Partial rows              |        2M |
| Final rows                |        1M |
| Shuffle records           |        2M |
| Shuffle bytes             |  17.4 MiB |
| Reduction                 |       80% |
| AQE final partitions      |         2 |
| AQE partition data        | ~17.9 MiB |
| Spill                     |       0 B |
| Sort fallback             |         0 |
| Partial hash probes       |      ~1.4 |
| Final hash probes         |      ~1.5 |
| Partial aggregation build |    ~2.1 s |
| Partial peak memory       |  ~192 MiB |
| Final aggregation build   |    ~1.4 s |
| Final peak memory         |  ~160 MiB |

The physical plan remains the same.

The workload has become significantly more expensive.

---

# 15. Query 3 — 5M Keys

The fourth workload uses:

```text
10M input rows
5M grouping keys
```

Average rows per key:

```text
10,000,000 / 5,000,000
= 2
```

Observed:

| Metric                    |     Result |
| ------------------------- | ---------: |
| Input rows                |        10M |
| Partial rows              |        10M |
| Final rows                |         5M |
| Shuffle records           |        10M |
| Shuffle bytes             |   90.1 MiB |
| Reduction                 |         0% |
| AQE original partitions   |          3 |
| AQE final partitions      |          2 |
| AQE partition data        |  ~92.2 MiB |
| Spill                     |        0 B |
| Sort fallback             |          0 |
| Partial hash probes       |       ~1.6 |
| Partial aggregation build |     ~4.4 s |
| Partial peak memory       | ~1,024 MiB |
| Final aggregation build   |     ~3.7 s |
| Final peak memory         |   ~584 MiB |

This is a major transition point.

Partial aggregation has effectively stopped reducing the number of records.

The experiment shows:

```text
10M input
   ↓
10M partial
```

Therefore:

```text
0% record reduction
```

---

# 16. Why 5M Keys Has Zero Reduction

With:

```text
5M keys
10M rows
```

there are only:

```text
2 rows/key
```

on average.

The deterministic modulo distribution means the same key does not provide enough local duplication to significantly reduce the number of records before the shuffle.

The result is:

```text
10M input records
        ↓
10M partial records
        ↓
Shuffle
```

Partial aggregation still exists in the physical plan.

But it is no longer providing meaningful record reduction.

This is a critical distinction:

> **The presence of a partial aggregation operator does not guarantee significant shuffle reduction.**

---

# 17. Query 4 — 10M Keys

The final workload uses:

```text
10M input rows
10M grouping keys
```

Average rows per key:

```text
10,000,000 / 10,000,000
= 1
```

Observed:

| Metric                    |     Result |
| ------------------------- | ---------: |
| Input rows                |        10M |
| Partial rows              |        10M |
| Final rows                |        10M |
| Shuffle records           |        10M |
| Shuffle bytes             |   90.0 MiB |
| Reduction                 |         0% |
| AQE original partitions   |          3 |
| AQE final partitions      |          2 |
| AQE partition data        |  ~91.3 MiB |
| Spill                     |        0 B |
| Sort fallback             |          0 |
| Partial hash probes       |       ~1.6 |
| Final hash probes         |       ~1.6 |
| Partial aggregation build |     ~4.8 s |
| Partial peak memory       | ~1,024 MiB |
| Final aggregation build   |     ~5.3 s |
| Final peak memory         | ~1,104 MiB |

This represents the extreme high-cardinality case.

There is effectively one group per input row.

Therefore:

```text
Partial aggregation cannot eliminate records.
```

---

# 18. Complete Experiment Matrix

The five workloads produce:

| Keys | Avg rows/key | Partial rows | Final rows | Shuffle records |  Shuffle | Reduction | AQE partitions |
| ---: | -----------: | -----------: | ---------: | --------------: | -------: | --------: | -------------: |
| 100K |          100 |         200K |       100K |            200K | 1.92 MiB |       98% |              1 |
| 500K |           20 |           1M |       500K |              1M |  8.7 MiB |       90% |              2 |
|   1M |           10 |           2M |         1M |              2M | 17.4 MiB |       80% |              2 |
|   5M |            2 |          10M |         5M |             10M | 90.1 MiB |        0% |              2 |
|  10M |            1 |          10M |        10M |             10M | 90.0 MiB |        0% |              2 |

This is the core result of Module 1.7.4.

---

# 19. The Cardinality Curve

The experiment shows three distinct regions.

## Region 1 — Low Cardinality

```text
100K keys
```

Results:

```text
98% reduction
1.92 MiB shuffle
```

Partial aggregation is highly effective.

---

## Region 2 — Medium Cardinality

```text
500K → 1M keys
```

Results:

```text
90% → 80% reduction
8.7 → 17.4 MiB shuffle
```

Partial aggregation is still valuable but progressively less effective.

---

## Region 3 — High Cardinality

```text
5M → 10M keys
```

Results:

```text
0% reduction
~90 MiB shuffle
```

Partial aggregation provides essentially no record-count reduction.

---

# 20. Reduction vs Cardinality

The observed relationship is:

```text
Keys       Reduction
--------------------
100K          98%
500K          90%
1M            80%
5M             0%
10M            0%
```

The important insight is that the relationship is **not linear**.

There is a substantial transition between:

```text
1M
```

and:

```text
5M
```

for this particular deterministic workload.

This should not be interpreted as a universal Spark threshold.

It is an observed characteristic of this workload and execution environment.

---

# 21. Shuffle Growth

Shuffle bytes increase substantially as cardinality rises.

```text
100K → 1.92 MiB
500K → 8.7 MiB
1M   → 17.4 MiB
5M   → 90.1 MiB
10M  → 90.0 MiB
```

The jump from:

```text
1M → 5M
```

is particularly important.

Shuffle increases from:

```text
17.4 MiB
```

to:

```text
90.1 MiB
```

because the partial aggregation stops eliminating records.

---

# 22. Memory Growth

Aggregation memory is even more revealing.

Maximum observed task memory increased approximately from:

```text
66 MiB
```

at 100K keys to:

```text
512 MiB
```

at 5M and 10M keys.

This is roughly:

```text
7.8×
```

the maximum task memory observed in the 100K-key case.

The final 10M-key aggregation reached approximately:

```text
1,104 MiB total
```

of peak aggregation memory.

This demonstrates that high-cardinality aggregation can become fundamentally memory-intensive even when no spill occurs in the current environment.

---

# 23. Why Memory Increases

Hash aggregation needs state for each group.

Conceptually:

```text
customer_id → aggregate buffer
```

At:

```text
100K groups
```

Spark maintains a relatively small number of aggregation entries.

At:

```text
10M groups
```

the number of aggregation entries becomes enormous.

Therefore:

```text
More groups
    ↓
Larger hash state
    ↓
More memory
```

The memory requirement is not determined only by input row count.

It is strongly influenced by **grouping cardinality**.

---

# 24. Hash Probe Behavior

The observed average hash probes were:

| Keys | Partial probes | Final probes |
| ---: | -------------: | -----------: |
| 100K |           ~1.1 |         ~1.4 |
| 500K |           ~1.3 |         ~1.5 |
|   1M |           ~1.4 |         ~1.5 |
|   5M |           ~1.6 |     ~1.4–1.5 |
|  10M |           ~1.6 |         ~1.6 |

The probe counts increase modestly.

However, the overall aggregation cost increases much more significantly.

This is another important engineering lesson:

> **A single operator metric should not be interpreted in isolation.**

Hash probes alone do not explain the full performance profile.

We also need:

```text
Cardinality
Memory
Build time
Shuffle
Task duration
AQE behavior
```

---

# 25. Aggregation Build Time

Partial aggregation build time increased from approximately:

```text
1.2 s
```

at 100K keys to:

```text
4.8 s
```

at 10M keys.

The progression was approximately:

| Keys | Partial build |
| ---: | ------------: |
| 100K |         1.2 s |
| 500K |         1.8 s |
|   1M |         2.1 s |
|   5M |         4.4 s |
|  10M |         4.8 s |

The aggregation becomes increasingly expensive as the hash state grows.

---

# 26. Final Aggregation Cost

The final aggregation also becomes more expensive.

Observed final aggregation build times included approximately:

```text
100K keys → 163 ms
500K keys → 439 ms
1M keys   → 1.4 s
5M keys   → 3.7 s
10M keys  → 5.3 s
```

This makes intuitive sense.

The final aggregation has more groups to maintain and merge as cardinality increases.

---

# 27. Whole-Stage Code Generation

The WholeStageCodegen metrics also increased with cardinality.

Representative observations:

### 100K

```text
~1.7 s
```

for the main code-generated stage.

### 1M

```text
~2.6 s
```

### 5M

```text
~6.7 s
```

### 10M

```text
~7.9 s
```

This is consistent with the increasing computational workload.

Again, these local timings are useful for understanding the trend but should not be treated as production performance numbers.

---

# 28. AQE Behavior

AQE behaved differently as shuffle volume increased.

Observed final AQE partitions:

```text
100K → 1
500K → 2
1M   → 2
5M   → 2
10M  → 2
```

The 5M and 10M cases initially produced approximately:

```text
3 AQE partitions
```

which were coalesced to:

```text
2
```

The key point is:

> **AQE adapts partitioning, but it cannot eliminate the fundamental cost of high aggregation cardinality.**

AQE can change how shuffle data is consumed.

It cannot turn:

```text
10M unique groups
```

into:

```text
100K groups
```

without changing the business semantics.

---

# 29. AQE Does Not Fix Cardinality

This is an important production lesson.

At 100K keys:

```text
~1.92 MiB shuffle
```

AQE can aggressively coalesce the data.

At 10M keys:

```text
~90 MiB shuffle
```

AQE still needs to process the data.

AQE helps optimize execution around the data.

It does not change the underlying cardinality of the aggregation.

Therefore:

```text
AQE ≠ substitute for data understanding
```

---

# 30. Spill Behavior

Despite the substantial memory increase, the experiment reported:

```text
Memory spill = 0 B
Disk spill   = 0 B
```

for all five workloads.

This means the local execution environment was able to complete these workloads without spilling.

However, this should **not** be interpreted as evidence that high-cardinality aggregation is safe in production.

A production cluster may have:

* smaller executor memory
* concurrent workloads
* multiple aggregations
* larger rows
* more grouping columns
* more complex aggregate buffers
* different JVM overhead

A 10M-group workload could therefore create significant memory pressure in production.

---

# 31. Why No Spill Does Not Mean No Risk

The 10M-key workload reached approximately:

```text
1,104 MiB
```

of total final aggregation peak memory.

The fact that it did not spill means only:

> **This particular workload completed within the available local execution memory.**

It does not mean:

```text
10M-group aggregation is inexpensive
```

or:

```text
10M groups will always fit in memory.
```

Memory consumption should be evaluated relative to the actual executor memory configuration and workload concurrency.

---

# 32. Physical Plan vs Runtime Behavior

The strongest finding in this experiment is:

```text
Physical plan:
essentially unchanged

Runtime:
dramatically changed
```

The execution strategy remains:

```text
HashAggregate
    ↓
Exchange
    ↓
HashAggregate
```

but the data flowing through the operators changes.

This means:

> **Physical plan analysis and runtime data analysis must be performed together.**

---

# 33. Same Plan, Different Workload

Compare:

```text id="8xtj55"
100K keys

10M input
200K partial
1.92 MiB shuffle
132 MiB peak partial memory
```

with:

```text id="b7tqir"
10M keys

10M input
10M partial
90 MiB shuffle
1,024 MiB peak partial memory
```

The physical plan family is the same.

The execution characteristics are completely different.

This is one of the most important lessons in Spark performance engineering.

---

# 34. Partial Aggregation Efficiency

A useful metric is:

```text id="1kphxy"
Reduction =
1 - (Partial Rows / Input Rows)
```

The experiment produces:

| Keys | Partial/Input | Reduction |
| ---: | ------------: | --------: |
| 100K |            2% |       98% |
| 500K |           10% |       90% |
|   1M |           20% |       80% |
|   5M |          100% |        0% |
|  10M |          100% |        0% |

This provides a simple way to reason about partial aggregation effectiveness.

---

# 35. Another Useful Metric

We can also look at:

```text id="e1dhh4"
Final groups / Input rows
```

For this workload:

```text
100K / 10M = 1%
500K / 10M = 5%
1M   / 10M = 10%
5M   / 10M = 50%
10M  / 10M = 100%
```

As the final group count approaches the input row count, there is progressively less opportunity for aggregation to reduce data.

---

# 36. Cardinality as a First-Class Performance Dimension

For aggregation workloads, cardinality should be treated as a first-class engineering metric.

Do not monitor only:

```text
Input rows
```

Also monitor:

```text
Distinct grouping keys
Rows per grouping key
Local distinct keys per partition
Partial aggregation output
```

A workload with:

```text
1B rows
1M groups
```

can behave very differently from:

```text
1B rows
900M groups
```

even though both contain exactly one billion input rows.

---

# 37. Production Diagnostic Pattern

When an aggregation becomes slower after a data-volume increase, investigate:

```text
Input volume
      ↓
Grouping cardinality
      ↓
Rows per group
      ↓
Partial aggregation output
      ↓
Shuffle records
      ↓
Shuffle bytes
      ↓
Aggregation memory
      ↓
Spill
      ↓
Task duration
```

This can quickly reveal whether the problem is:

* larger input
* higher cardinality
* worse duplication
* skew
* insufficient partitioning
* memory pressure

---

# 38. Important Engineering Distinction

There are two different questions:

### Question 1

> How much data entered Spark?

Answer:

```text
10M rows
```

### Question 2

> How much data survived local aggregation?

Answers:

```text
200K
1M
2M
10M
10M
```

The second metric is often much more useful when diagnosing distributed aggregation performance.

---

# 39. Cardinality Transition

This experiment shows a clear transition:

```text
100K
  ↓
500K
  ↓
1M
  ↓
5M
  ↓
10M
```

The corresponding partial aggregation behavior is:

```text
Highly effective
       ↓
Effective
       ↓
Less effective
       ↓
Ineffective
       ↓
Ineffective
```

This is the central result of Module 1.7.4.

---

# 40. Experiment Result Summary

The complete matrix:

```text id="h43jba"
100K keys
10M input
200K partial
1.92 MiB shuffle
98% reduction

500K keys
10M input
1M partial
8.7 MiB shuffle
90% reduction

1M keys
10M input
2M partial
17.4 MiB shuffle
80% reduction

5M keys
10M input
10M partial
90.1 MiB shuffle
0% reduction

10M keys
10M input
10M partial
90.0 MiB shuffle
0% reduction
```

---

# 41. Engineering Findings

## Finding 1 — Cardinality dominates aggregation behavior

With the input fixed at 10M rows, increasing grouping cardinality dramatically changed:

* shuffle
* memory
* build time
* codegen time

---

## Finding 2 — Partial aggregation benefit collapses at high cardinality

Observed reduction:

```text
98%
 ↓
90%
 ↓
80%
 ↓
0%
 ↓
0%
```

---

## Finding 3 — Shuffle volume increases dramatically

Observed shuffle:

```text
1.92 MiB
 ↓
8.7 MiB
 ↓
17.4 MiB
 ↓
90.1 MiB
 ↓
90.0 MiB
```

---

## Finding 4 — Memory grows with the number of groups

Maximum observed task aggregation memory increased from approximately:

```text
66 MiB
```

to:

```text
512 MiB
```

at the high-cardinality workloads.

---

## Finding 5 — Physical plan alone is insufficient

The physical plan remained essentially unchanged.

The runtime cost did not.

---

## Finding 6 — AQE helps partition consumption but does not solve cardinality

AQE reduced final shuffle-read partitions.

But it could not eliminate the underlying high-cardinality aggregation work.

---

# 42. Engineering Decision

For production aggregation pipelines:

> **Always measure grouping cardinality and partial aggregation effectiveness before tuning shuffle partitions or executor resources.**

A practical diagnostic table is:

| Metric                  | Why it matters             |
| ----------------------- | -------------------------- |
| Input rows              | Total workload             |
| Distinct groups         | Aggregation state size     |
| Rows/group              | Aggregation opportunity    |
| Partial rows            | Local reduction            |
| Shuffle records         | Network/shuffle workload   |
| Shuffle bytes           | Actual data volume         |
| Peak aggregation memory | Memory pressure            |
| Spill                   | Memory insufficiency       |
| AQE partitions          | Runtime partition behavior |
| Task duration           | Execution imbalance        |

---

# 43. What This Means for Spark Tuning

If you observe:

```text
High input
Low cardinality
Large shuffle
```

investigate why partial aggregation is not reducing the data as expected.

If you observe:

```text
High cardinality
Large aggregation memory
Large shuffle
```

the issue may be inherent to the workload rather than simply poor Spark configuration.

If you observe:

```text
High cardinality
Severe skew
Long-running tasks
```

the next investigation should focus on skew.

---

# 44. What Not to Conclude

Do not conclude:

> "Spark cannot handle high-cardinality aggregation."

That would be too broad.

The experiment only demonstrates:

> **High-cardinality aggregation creates significantly more aggregation state and provides less opportunity for local record reduction for this workload.**

Production performance depends on:

* cluster size
* executor memory
* CPU
* data types
* number of grouping columns
* aggregate functions
* partitioning
* skew
* concurrency
* Spark configuration

---

# 45. Local Benchmark Limitation

All measurements were collected on a local Windows environment.

Therefore, exact timings should not be generalized to a production cluster.

The strongest conclusions are the **relative relationships**:

```text
Cardinality ↑
       ↓
Partial reduction ↓
       ↓
Shuffle ↑
       ↓
Memory ↑
       ↓
Aggregation cost ↑
```

Those relationships are more important than the absolute local timings.

---

# 46. Reproducibility

Environment:

```text
Apache Spark 3.5.1
Scala 2.12.18
Java 17.0.20.101
Gradle 9.6.0
```

Run:

```bash id="k6v0ei"
./gradlew compileScala
```

Then:

```bash id="0zqzpk"
./gradlew runAggregationCardinalityExperiment --args="10000000 20 --ui-pause=300"
```

The experiment executes all five cardinalities:

```text
100K
500K
1M
5M
10M
```

using the same input volume and shuffle configuration.

---

# 47. Reproducibility Principle

The experiment deliberately keeps:

```text
Input rows = 10M
Shuffle partitions = 20
Aggregation = SUM
Grouping expression = customer_id
```

constant.

Only:

```text
Number of grouping keys
```

changes.

This makes the experiment suitable for isolating the effect of aggregation cardinality.

---

# 48. Execution Diagram

```text
                 10M Input Rows
                       │
                       ▼
                ┌────────────┐
                │   Range    │
                └─────┬──────┘
                      │
                      ▼
                ┌────────────┐
                │  Project   │
                └─────┬──────┘
                      │
                      ▼
          ┌────────────────────────┐
          │ Partial HashAggregate  │
          │                        │
          │ 100K → 200K rows       │
          │ 500K → 1M rows         │
          │ 1M   → 2M rows         │
          │ 5M   → 10M rows        │
          │ 10M  → 10M rows        │
          └───────────┬────────────┘
                      │
                      ▼
                ┌────────────┐
                │  Exchange  │
                └─────┬──────┘
                      │
                      ▼
                ┌────────────┐
                │    AQE     │
                │ ShuffleRead│
                └─────┬──────┘
                      │
                      ▼
          ┌────────────────────────┐
          │ Final HashAggregate    │
          └───────────┬────────────┘
                      │
                      ▼
                 Final Groups
```

---

# 49. The Big Picture

The five workloads can be visualized conceptually as:

```text
                    Cardinality
                        ↑

100K ─────────────────────────────────
      98% reduction
      1.92 MiB shuffle
      ~132 MiB partial memory

500K ─────────────────────────────
      90% reduction
      8.7 MiB shuffle

1M ───────────────────────────
      80% reduction
      17.4 MiB shuffle
      ~192 MiB partial memory

5M ───────────────
      0% reduction
      90.1 MiB shuffle
      ~1 GiB partial memory

10M ──────────────
      0% reduction
      90.0 MiB shuffle
      ~1 GiB partial memory
```

The key transition is clear:

```text
Aggregation-friendly
        ↓
Less aggregation-friendly
        ↓
High-cardinality
        ↓
Almost no local reduction
```

---

# 50. Relationship to Previous Experiments

Module 1.7.1 established the baseline:

```text
GROUP BY + SUM
```

Module 1.7.2 showed:

```text
HashAggregate
```

Module 1.7.3 demonstrated:

```text
Partial aggregation
```

Module 1.7.4 now demonstrates:

```text
Cardinality
      ↓
Partial aggregation effectiveness
      ↓
Shuffle
      ↓
Memory
      ↓
Runtime
```

This creates a coherent progression:

```text
1.7.1 Baseline
       ↓
1.7.2 Hash Aggregation
       ↓
1.7.3 Partial Aggregation
       ↓
1.7.4 Aggregation Cardinality
```

---

# 51. Production Architecture Implication

For a production financial aggregation pipeline:

```text
Raw Transactions
       │
       ▼
Filtering / Projection
       │
       ▼
Partial Aggregation
       │
       ▼
Shuffle
       │
       ▼
Final Aggregation
       │
       ▼
Customer / Account Metrics
```

the cardinality of:

```text
customer_id
account_id
trade_id
portfolio_id
instrument_id
```

can materially influence execution cost.

A pipeline grouping by:

```text
customer_id
```

may be very efficient.

A pipeline grouping by:

```text
transaction_id
```

may have almost no aggregation opportunity if transaction IDs are unique.

The business meaning of the grouping key therefore has direct execution consequences.

---

# 52. Engineering Principle

> **Aggregation performance is fundamentally influenced by the relationship between input volume and grouping cardinality.**

A useful mental model is:

```text
Input volume
      +
Grouping cardinality
      +
Local key distribution
      ↓
Partial aggregation effectiveness
      ↓
Shuffle volume
      ↓
Memory pressure
      ↓
Runtime
```

Therefore:

> **When an aggregation slows down, ask "How many groups are there?" before asking "How many shuffle partitions should I use?"**

---

# 53. Final Engineering Decision

For production Spark workloads:

1. Measure input volume.
2. Measure grouping cardinality.
3. Measure rows per group.
4. Inspect partial aggregation output.
5. Measure shuffle records and bytes.
6. Measure aggregation memory.
7. Check for spill.
8. Inspect AQE behavior.
9. Only then tune partitioning and resources.

The key lesson from this experiment is:

> **A correct physical plan can still become expensive when the data cardinality changes.**

The optimizer chooses a strategy.

The data determines how expensive that strategy becomes.

---

# 54. Next Experiment

The next experiment moves from **cardinality** to **shuffle partition sizing**.

The question becomes:

> **If the aggregation workload remains constant, what happens when we configure 10, 20, 50, 100, and 200 shuffle partitions?**

That experiment will demonstrate another important Spark concept:

```text
Configured shuffle partitions
              ≠
Actual Exchange partition count
              ≠
Final AQE read partitions
```

We will validate all three explicitly using the physical plan and Spark UI.

---

# 55. Final Engineering Principle

> **Don't optimize Spark aggregation by looking only at the SQL or only at the physical plan. Understand the data shape, especially grouping cardinality, and validate the resulting execution with runtime evidence.**

For this experiment:

```text
10M input rows
```

produced:

```text
100K keys  → 200K partial rows → 98% reduction
500K keys  → 1M partial rows   → 90% reduction
1M keys    → 2M partial rows   → 80% reduction
5M keys    → 10M partial rows  → 0% reduction
10M keys   → 10M partial rows  → 0% reduction
```

The physical plan stayed essentially the same.

The workload did not.

**That is the essence of data-aware Spark performance engineering.**
