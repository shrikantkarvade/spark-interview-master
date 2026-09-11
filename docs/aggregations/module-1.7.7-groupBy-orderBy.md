# Module 1.7.7 — GROUP BY + ORDER BY

## Experiment Objective

Understand how Apache Spark executes a query that combines:

```sql
SUM(amount)
GROUP BY customer_id
ORDER BY SUM(amount) DESC
```

The experiment demonstrates an important distinction between:

* **Aggregation partitioning** — data must be distributed by `customer_id`
* **Global ordering** — aggregated results must subsequently be distributed according to the ordering expression

The primary objective is to determine how adding a global `ORDER BY` changes Spark's physical execution plan, shuffle behavior, partitioning strategy, and execution characteristics.

---

# 1. Business Use Case

Consider a financial transaction platform containing billions of transactions.

A common analytical requirement is:

> Find customers with the highest total transaction value.

Conceptually:

```sql
SELECT
    customer_id,
    SUM(amount) AS total_amount
FROM transactions
GROUP BY customer_id
ORDER BY total_amount DESC;
```

This looks like one logical operation to an application developer.

Spark, however, must solve two separate distributed-computing problems:

```text
Problem 1:
Which executor should process each customer's aggregation?

Problem 2:
How can Spark produce one globally ordered result?
```

These requirements can result in **multiple shuffle stages**.

---

# 2. Experiment Questions

This experiment answers the following questions:

1. Does `GROUP BY` still use two-phase `HashAggregate`?
2. How many shuffle stages are introduced?
3. What partitioning strategy is used for `GROUP BY`?
4. What partitioning strategy is used for global `ORDER BY`?
5. Does `ORDER BY` introduce a `Sort` operator?
6. How many records are shuffled for the ordering operation?
7. Does AQE coalesce the ordering shuffle?
8. Does global ordering require sorting the original 10M transaction rows?
9. Does Spark aggregate the data before performing the global ordering?
10. What are the production implications of adding a global `ORDER BY`?

---

# 3. Experiment Design

## Dataset

The experiment uses deterministic synthetic transaction data.

| Parameter                     |      Value |
| ----------------------------- | ---------: |
| Transaction count             | 10,000,000 |
| Customer count                |  1,000,000 |
| Configured shuffle partitions |        200 |
| AQE                           |    Enabled |
| AQE coalescing                |    Enabled |
| Input partitions              |          2 |

Each transaction is generated using:

```text
customer_id = transaction_id % 1,000,000
amount      = transaction_id % 1,000
```

This produces:

```text
10,000,000 transaction rows
        ↓
1,000,000 distinct customers
```

Each customer therefore receives approximately 10 transactions.

---

# 4. Experiment Matrix

Two executions are compared.

## Run 1 — GROUP BY Only

```text
transactions
    ↓
GROUP BY customer_id
    ↓
SUM(amount)
```

Expected behavior:

```text
Partial HashAggregate
        ↓
Hash Shuffle by customer_id
        ↓
Final HashAggregate
```

---

## Run 2 — GROUP BY + ORDER BY

```text
transactions
    ↓
GROUP BY customer_id
    ↓
SUM(amount)
    ↓
ORDER BY SUM(amount) DESC
```

Expected additional work:

```text
Partial HashAggregate
        ↓
Hash Shuffle by customer_id
        ↓
Final HashAggregate
        ↓
Range Shuffle by aggregate value
        ↓
Sort
```

---

# 5. Data Generation

The experiment generates deterministic data:

```scala
val transactions =
  spark.range(0, transactionCount)
    .selectExpr(
      "id AS transaction_id",
      s"id % $customerCount AS customer_id",
      "id % 1000 AS amount"
    )
```

This is intentionally deterministic so that Run 1 and Run 2 can be compared without introducing randomness.

---

# 6. Run 1 — GROUP BY Only

The first query is:

```scala
val aggregation =
  transactions
    .groupBy("customer_id")
    .sum("amount")
```

Spark initially generates the following logical execution structure:

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

The important Exchange is:

```text
hashpartitioning(customer_id, 200)
```

---

# 7. Run 1 — Observed Results

The observed execution produced:

| Metric                        |     Result |
| ----------------------------- | ---------: |
| Input rows                    | 10,000,000 |
| Configured shuffle partitions |        200 |
| Aggregation shuffle records   |  2,000,000 |
| Aggregation shuffle size      |   18.7 MiB |
| Final output rows             |  1,000,000 |
| AQE final partitions          |          2 |
| Spill                         |        0 B |
| Sort operator                 |       None |

The aggregation reduced:

```text
10,000,000 input rows
        ↓
2,000,000 shuffle records
```

before the final aggregation.

Therefore approximately:

```text
10M / 2M = 5×
```

record reduction occurred before the aggregation shuffle.

---

# 8. Run 1 — AQE Behavior

The Exchange was configured for:

```text
200 partitions
```

but the Spark UI reported:

```text
AQEShuffleRead
number of partitions: 2
number of coalesced partitions: 2
```

Therefore:

```text
Configured partitions: 200
Final AQE partitions:   2
```

AQE determined that the actual shuffle data volume was small enough to be processed using substantially fewer partitions.

This demonstrates an important distinction:

> `spark.sql.shuffle.partitions = 200` is the configured shuffle parallelism, not necessarily the number of final partitions consumed by the adaptive query.

---

# 9. Run 2 — GROUP BY + ORDER BY

The second query adds:

```scala
.orderBy(
  org.apache.spark.sql.functions.desc("sum(amount)")
)
```

The final physical plan is:

```text
Range
  ↓
Project
  ↓
Partial HashAggregate
  ↓
Exchange: hashpartitioning(customer_id, 200)
  ↓
Final HashAggregate
  ↓
Exchange: rangepartitioning(sum(amount) DESC, 200)
  ↓
AQEShuffleRead
  ↓
Sort
  ↓
Result
```

This is the key result of Module 1.7.7.

---

# 10. Run 2 — Actual Physical Plan

The Spark UI reported the following final adaptive plan:

```text
== Physical Plan ==
AdaptiveSparkPlan (18)
+- == Final Plan ==
   DeserializeToObject (12)
   +- * Sort (11)
      +- AQEShuffleRead (10)
         +- ShuffleQueryStage (9), Statistics(sizeInBytes=22.9 MiB, rowCount=1.00E+6)
            +- Exchange (8)
               +- * HashAggregate (7)
                  +- AQEShuffleRead (6)
                     +- ShuffleQueryStage (5), Statistics(sizeInBytes=45.8 MiB, rowCount=2.00E+6)
                        +- Exchange (4)
                           +- * HashAggregate (3)
                              +- * Project (2)
                                 +- * Range (1)
```

The most important operators are:

```text
(4) Exchange
hashpartitioning(customer_id, 200)
```

and:

```text
(8) Exchange
rangepartitioning(sum(amount) DESC NULLS LAST, 200)
```

followed by:

```text
(11) Sort
[sum(amount) DESC NULLS LAST]
```

---

# 11. Two Different Shuffles

This experiment clearly demonstrates that the two operations require different data-distribution strategies.

## Shuffle #1 — GROUP BY

```text
Exchange
hashpartitioning(customer_id, 200)
```

Purpose:

```text
All records for the same customer
must reach the same aggregation partition.
```

Conceptually:

```text
customer_id
     ↓
hash(customer_id)
     ↓
partition
```

---

## Shuffle #2 — ORDER BY

```text
Exchange
rangepartitioning(sum(amount) DESC, 200)
```

Purpose:

```text
Distribute aggregated rows according to
their ordering value so that Spark can
produce a globally ordered result.
```

Conceptually:

```text
sum(amount)
     ↓
range partitioning
     ↓
ordered ranges
     ↓
Sort within partitions
```

These are fundamentally different partitioning requirements.

---

# 12. Why Does ORDER BY Require Another Shuffle?

Suppose the aggregation produces:

```text
Partition 1:

Customer A → 1,000
Customer B →   200


Partition 2:

Customer C → 800
Customer D → 100
```

If each partition independently sorts its data:

```text
Partition 1:
1,000
200

Partition 2:
800
100
```

the overall result is **not globally sorted**.

The driver cannot simply concatenate the partitions:

```text
1,000
200
800
100
```

because:

```text
200 > 800
```

is false.

Spark therefore needs another distribution step so that ordering ranges are assigned appropriately.

The final execution becomes:

```text
Aggregation
    ↓
Range Partitioning
    ↓
Sort
    ↓
Globally ordered result
```

---

# 13. Important Optimization — ORDER BY Happens After Aggregation

One of the most valuable observations from the physical plan is that Spark does **not** globally sort the original 10M transactions.

Instead:

```text
10,000,000 transactions
        ↓
Partial aggregation
        ↓
Aggregation shuffle
        ↓
Final aggregation
        ↓
1,000,000 customer rows
        ↓
Ordering shuffle
        ↓
Sort
```

Therefore the ordering shuffle operates on approximately:

```text
1,000,000 rows
```

rather than:

```text
10,000,000 rows
```

This is an important distributed-query optimization.

The aggregation reduces the dataset before the global ordering operation.

---

# 14. Run 2 — Ordering Shuffle Metrics

The Spark UI reported:

```text
shuffle records written: 1,000,000
```

and:

```text
shuffle bytes written: 8.0 MiB
```

The ordering Exchange was configured for:

```text
200 partitions
```

and AQE subsequently coalesced the read to:

```text
2 partitions
```

Therefore:

```text
ORDER BY shuffle

1,000,000 records
8.0 MiB
200 configured partitions
2 final AQE partitions
```

---

# 15. Run 2 — AQE Coalescing

The final plan contains:

```text
AQEShuffleRead
Arguments: coalesced
```

with:

```text
number of partitions: 2
number of coalesced partitions: 2
```

This confirms that AQE was active for the ordering shuffle as well.

The important architectural point is:

```text
Exchange
  ↓
Shuffle data
  ↓
AQE analyzes shuffle statistics
  ↓
Coalesces small partitions
  ↓
AQEShuffleRead
```

AQE reduces unnecessary downstream partition parallelism.

However:

> AQE coalescing does not remove the ordering shuffle itself.

The second Exchange is still present in the final plan.

---

# 16. Sort Operator

Run 1 contained no Sort operator.

Run 2 contains:

```text
Sort
Arguments:
[sum(amount) DESC NULLS LAST]
```

Therefore the experiment conclusively demonstrates:

```text
GROUP BY
→ HashAggregate

GROUP BY + ORDER BY
→ HashAggregate
→ Range Exchange
→ Sort
```

This is the primary physical-plan difference between the two executions.

---

# 17. Run 2 — Execution Metrics

The Spark UI reported:

| Metric                   |     Run 2 |
| ------------------------ | --------: |
| Query duration           |      ~3 s |
| First shuffle records    | 2,000,000 |
| First shuffle size       |  18.7 MiB |
| Second shuffle records   | 1,000,000 |
| Second shuffle size      |   8.0 MiB |
| AQE final partitions     |         2 |
| Spill                    |       0 B |
| Sort fallback tasks      |         0 |
| Final aggregation output | 1,000,000 |

The aggregation stage reported approximately:

```text
2,000,000 output rows
```

before the final aggregation.

The final aggregation then produced:

```text
1,000,000 rows
```

which became the input to the ordering shuffle.

---

# 18. Memory and Spill Behavior

The Spark UI reported:

```text
Spill size: 0 B
```

for the aggregation operators.

It also reported:

```text
Sort fallback tasks: 0
```

and no sort spill.

Therefore this workload comfortably fit within the available memory in the local test environment.

This should not be interpreted as:

> ORDER BY never causes memory pressure.

Instead:

> This particular dataset and execution environment did not produce memory pressure or spilling.

At production scale, the amount of aggregated data, number of ordering keys, partition sizes, executor memory, and cluster parallelism can materially change the behavior.

---

# 19. Hash Aggregation Remains Efficient

Even after adding `ORDER BY`, Spark continues to use:

```text
HashAggregate
```

for the aggregation.

The final plan contains:

```text
(3) HashAggregate
Functions:
partial_sum(amount)
```

followed by:

```text
(7) HashAggregate
Functions:
sum(amount)
```

Therefore:

```text
ORDER BY
```

does not force the aggregation itself to become a sort-based aggregation.

Instead, Spark performs:

```text
HashAggregate
       ↓
Range Exchange
       ↓
Sort
```

This separation of concerns is important when reading Spark physical plans.

---

# 20. Run 1 vs Run 2

| Characteristic              | Run 1 — GROUP BY | Run 2 — GROUP BY + ORDER BY |
| --------------------------- | ---------------- | --------------------------- |
| Input rows                  | 10M              | 10M                         |
| Final rows                  | 1M               | 1M                          |
| Partial HashAggregate       | Yes              | Yes                         |
| Final HashAggregate         | Yes              | Yes                         |
| GROUP BY shuffle            | Yes              | Yes                         |
| GROUP BY partitioning       | Hash             | Hash                        |
| GROUP BY shuffle records    | 2M               | 2M                          |
| GROUP BY shuffle size       | 18.7 MiB         | 18.7 MiB                    |
| Additional ordering shuffle | No               | **Yes**                     |
| Ordering partitioning       | —                | **Range**                   |
| Ordering shuffle records    | —                | **1M**                      |
| Ordering shuffle size       | —                | **8.0 MiB**                 |
| Sort operator               | No               | **Yes**                     |
| AQE                         | Enabled          | Enabled                     |
| AQE coalescing              | 200 → 2          | 200 → 2                     |
| Spill                       | 0 B              | 0 B                         |

---

# 21. Execution Architecture

The complete Run 2 execution can be visualized as:

```text
                 10M TRANSACTIONS
                        │
                        ▼
                   Range
                        │
                        ▼
                    Project
                        │
                        ▼
              Partial HashAggregate
                        │
                        │
                        ▼
          ┌───────────────────────────┐
          │ Shuffle #1                │
          │ hashpartition(customer_id)│
          │ 200 configured partitions │
          └───────────────────────────┘
                        │
                        ▼
               AQE Shuffle Read
                  coalesced
                     200 → 2
                        │
                        ▼
                Final HashAggregate
                        │
                        ▼
                 1M customer rows
                        │
                        ▼
          ┌───────────────────────────┐
          │ Shuffle #2                │
          │ range partitioning       │
          │ sum(amount) DESC         │
          │ 200 configured partitions │
          └───────────────────────────┘
                        │
                        ▼
               AQE Shuffle Read
                  coalesced
                     200 → 2
                        │
                        ▼
                       Sort
                        │
                        ▼
              Globally ordered result
```

---

# 22. Spark Internals — Why the Two Exchanges Exist

A distributed aggregation requires:

```text
hashpartitioning(customer_id)
```

because all rows for a particular customer need to be colocated.

A global ordering requires:

```text
rangepartitioning(sum(amount) DESC)
```

because Spark needs to establish ordering ranges across partitions.

Therefore the physical plan contains two distinct data redistribution operations:

```text
GROUP BY
   ↓
Hash Exchange

ORDER BY
   ↓
Range Exchange
```

The key lesson is:

> Logical SQL operations can translate into independent physical distribution requirements.

---

# 23. Why `ORDER BY` Can Be Expensive

A global:

```sql
ORDER BY
```

is fundamentally different from simply sorting within each partition.

Global ordering can require:

```text
Shuffle
+
Partition boundary determination
+
Data movement
+
Sorting
```

Therefore global ordering should be treated carefully in large-scale Spark workloads.

For example:

```sql
SELECT *
FROM huge_table
ORDER BY transaction_amount DESC;
```

may require a large amount of distributed work.

---

# 24. Prefer Top-N When Full Ordering Is Not Required

A common production mistake is using:

```sql
ORDER BY total_amount DESC
```

when the business requirement is actually:

> Give me the top 100 customers.

Those are different requirements.

If only a small number of records are needed, consider:

```sql
ORDER BY total_amount DESC
LIMIT 100
```

This changes the optimization opportunity because Spark can potentially avoid materializing and processing the complete globally ordered result in the same way as an unrestricted global sort.

This becomes the subject of the next optimization-oriented experiment:

**Module 1.7.8 — Top-N Aggregation.**

---

# 25. Production Engineering Takeaways

## 25.1 GROUP BY and ORDER BY have different distribution requirements

```text
GROUP BY customer_id
        ↓
Hash partitioning

ORDER BY total_amount
        ↓
Range partitioning + Sort
```

---

## 25.2 ORDER BY can introduce an additional shuffle

The experiment proves that:

```text
GROUP BY
```

requires one shuffle, while:

```text
GROUP BY + ORDER BY
```

requires another Exchange for global ordering.

---

## 25.3 Aggregate before sorting

A strong Spark optimization principle is:

```text
Reduce data first
        ↓
Sort later
```

In this experiment:

```text
10M transactions
        ↓
1M customer aggregates
        ↓
ORDER BY
```

The ordering stage therefore operates on the smaller aggregated dataset.

---

## 25.4 AQE does not eliminate the shuffle

AQE changed:

```text
200 partitions
```

to:

```text
2 final partitions
```

but the Exchange remained.

Therefore:

```text
AQE coalescing ≠ shuffle elimination
```

AQE optimizes how the shuffle output is consumed; it does not make the required global data redistribution disappear.

---

## 25.5 Small local workloads can hide the cost

This experiment ran successfully with relatively small shuffle sizes:

```text
18.7 MiB
+
8.0 MiB
```

and no spill.

At production scale, the same logical pattern could involve:

```text
GBs / TBs of shuffle
+
large network transfer
+
large sort workload
+
executor memory pressure
```

Therefore query behavior should be evaluated using production-scale data characteristics rather than only local execution time.

---

# 26. Engineering Decision Framework

When reviewing a Spark query containing `ORDER BY`, ask:

### Question 1

Do we really need global ordering?

If not, avoid it.

---

### Question 2

Do we only need the top N?

If yes, investigate:

```text
ORDER BY ... LIMIT N
```

rather than returning the entire ordered dataset.

---

### Question 3

Can aggregation/filtering happen before ordering?

Prefer:

```text
Filter
   ↓
Aggregate
   ↓
Order
```

over:

```text
Order
   ↓
Aggregate
```

when logically equivalent.

---

### Question 4

How large is the post-aggregation dataset?

The cost of global ordering depends heavily on the amount of data that reaches the ordering stage.

---

### Question 5

What does the physical plan show?

Look specifically for:

```text
Exchange
rangepartitioning(...)
Sort
```

These are strong indicators that a global ordering requirement has introduced distributed work.

---

# 27. Spark UI Evidence Checklist

For this experiment, the following evidence should be captured in the Spark UI.

### Run 1

```text
SQL Query
    ↓
HashAggregate
    ↓
Exchange hashpartitioning(customer_id)
```

Record:

* query duration
* shuffle records
* shuffle bytes
* number of partitions
* AQE coalescing
* aggregation memory
* spill

---

### Run 2

Look specifically for:

```text
Exchange hashpartitioning(customer_id)
        ↓
HashAggregate
        ↓
Exchange rangepartitioning(sum(amount) DESC)
        ↓
Sort
```

Record:

* first shuffle size
* second shuffle size
* first shuffle records
* second shuffle records
* AQE coalescing
* sort operator
* spill
* final output rows

---

# 28. Final Findings

The experiment conclusively demonstrated that adding global ordering changes Spark's physical execution strategy.

### GROUP BY only

```text
Partial HashAggregate
        ↓
Hash Exchange
        ↓
Final HashAggregate
```

### GROUP BY + ORDER BY

```text
Partial HashAggregate
        ↓
Hash Exchange
        ↓
Final HashAggregate
        ↓
Range Exchange
        ↓
AQE Shuffle Read
        ↓
Sort
```

The second shuffle operated on:

```text
1,000,000 aggregated rows
8.0 MiB
```

rather than the original:

```text
10,000,000 transaction rows
```

AQE subsequently coalesced the configured 200 partitions to 2 final partitions for the shuffle reads.

---

# 29. Core Learning

The most important concept from Module 1.7.7 is:

> **`GROUP BY` and global `ORDER BY` impose different data-distribution requirements, so Spark may need separate shuffle stages for each.**

The physical execution demonstrates:

```text
GROUP BY
    ↓
Hash Partitioning
    ↓
Aggregation
    ↓
ORDER BY
    ↓
Range Partitioning
    ↓
Sort
```

Therefore, when analyzing Spark performance, do not look only at the SQL syntax.

Always ask:

```text
What partitioning does this operation require?

Does it require a shuffle?

How much data is shuffled?

Can AQE reduce the downstream partition count?

Can the dataset be reduced before the shuffle?

Do we actually need global ordering?
```

These questions are critical when optimizing production Spark pipelines.

---

# 30. Interview Questions

### Q1. Why does `GROUP BY` require a shuffle?

Because records belonging to the same grouping key must be colocated so that Spark can compute the final aggregate correctly.

---

### Q2. Why does `ORDER BY` require another shuffle?

Because global ordering cannot be guaranteed when independently sorted partitions contain overlapping value ranges. Spark must redistribute records according to the ordering expression.

---

### Q3. What is the difference between hash partitioning and range partitioning?

**Hash partitioning** distributes records according to a hash of a key:

```text
hash(customer_id)
```

**Range partitioning** distributes records according to value ranges:

```text
amount range 1
amount range 2
amount range 3
...
```

Range partitioning is particularly useful for ordered data.

---

### Q4. Does AQE remove the ORDER BY shuffle?

No.

AQE can coalesce small shuffle partitions, but the required Exchange remains.

---

### Q5. Why is Spark sorting 1M rows instead of 10M rows?

Because Spark performs the aggregation first and the ordering is applied to the aggregated result.

---

### Q6. What operators should you look for when diagnosing an expensive global ORDER BY?

Primarily:

```text
Exchange
rangepartitioning(...)
Sort
```

and the associated shuffle metrics.

---

### Q7. Is `ORDER BY` always bad?

No.

Global ordering is perfectly valid when required. The engineering concern is understanding and managing its distributed execution cost.

---

### Q8. What would you do if the business only needs the top 100 customers?

Investigate a Top-N pattern:

```sql
ORDER BY total_amount DESC
LIMIT 100
```

rather than returning the entire globally ordered dataset.

---

# 31. Portfolio-Level Engineering Summary

This experiment demonstrates an important real-world Spark optimization concept:

```text
Logical SQL:

GROUP BY + ORDER BY
```

can become:

```text
Partial Aggregation
        ↓
Hash Shuffle
        ↓
Final Aggregation
        ↓
Range Shuffle
        ↓
Sort
```

The ability to identify these physical execution boundaries is essential for diagnosing Spark performance.

A production Spark engineer should therefore reason from:

```text
SQL
 ↓
Logical Plan
 ↓
Physical Plan
 ↓
Exchange boundaries
 ↓
Shuffle volume
 ↓
Partition sizing
 ↓
AQE behavior
 ↓
Executor resource usage
```

rather than treating SQL syntax as an indication of actual execution cost.

---

# 32. Experiment Conclusion

**Module 1.7.7 successfully demonstrated the physical impact of adding a global `ORDER BY` to an aggregation query.**

The experiment established that:

* Spark uses two-phase `HashAggregate`.
* `GROUP BY` introduces a hash-partitioning shuffle.
* `ORDER BY` introduces a second range-partitioning shuffle.
* The ordering shuffle operates after aggregation.
* The ordering stage contains a `Sort` operator.
* AQE coalesces the configured 200 partitions to 2 in this workload.
* No spill occurred in the tested environment.
* The additional ordering shuffle processed 1M aggregated records rather than 10M source records.
* Global ordering therefore represents an additional distributed execution boundary that should be evaluated carefully in production.

### Key principle

> **Aggregate first, reduce the dataset, and globally order only when the business requirement genuinely needs it.**

---

## Next Module

### Module 1.7.8 — Top-N Aggregation

The next experiment will investigate the difference between:

```sql
ORDER BY total_amount DESC
```

and:

```sql
ORDER BY total_amount DESC
LIMIT N
```

with particular focus on:

* Top-N optimization
* `Sort` vs bounded sorting
* shuffle behavior
* AQE
* memory consumption
* full sort vs partial/top-N execution
* production patterns for leaderboard/ranking workloads
