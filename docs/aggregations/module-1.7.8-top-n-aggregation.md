# Module 1.7.8 — Top-N Aggregation

## Experiment Objective

Understand how Apache Spark executes Top-N queries and why:

```text
GROUP BY + ORDER BY + LIMIT N
```

can be substantially more efficient than:

```text
GROUP BY + ORDER BY
```

when the business requirement only needs the highest or lowest `N` results.

This experiment compares:

1. **Full Global ORDER BY**
2. **ORDER BY + LIMIT N**

The experiment specifically investigates whether Spark can replace:

```text
Range Partitioning Shuffle
        ↓
Global Sort
```

with the specialized:

```text
TakeOrderedAndProject
```

execution strategy.

---

# 1. Business Use Case

Consider a financial transaction platform containing millions of transactions.

A business analyst asks:

> "Show me the top 100 customers by total transaction amount."

A naïve implementation might retrieve every aggregated customer and globally sort the entire result:

```text
10M transactions
       ↓
GROUP BY customer
       ↓
1M customers
       ↓
GLOBAL ORDER BY
       ↓
1M sorted customers
```

However, the business only needs:

```text
Top 100 customers
```

Therefore sorting all 1M customers is unnecessary work.

The optimized requirement is:

```text
10M transactions
       ↓
GROUP BY customer
       ↓
1M customers
       ↓
TOP 100
```

This experiment demonstrates how Spark recognizes and optimizes this pattern.

---

# 2. Queries Under Test

## Run 1 — Full Global ORDER BY

```scala
transactions
  .groupBy("customer_id")
  .sum("amount")
  .orderBy(desc("sum(amount)"))
```

This returns the complete set of customers in descending order.

Expected execution characteristics:

```text
HashAggregate
      ↓
Exchange
      ↓
HashAggregate
      ↓
Range Partitioning Shuffle
      ↓
Sort
      ↓
Complete ordered result
```

---

## Run 2 — Top-N

```scala
transactions
  .groupBy("customer_id")
  .sum("amount")
  .orderBy(desc("sum(amount)"))
  .limit(100)
```

Expected optimized execution:

```text
HashAggregate
      ↓
Exchange
      ↓
HashAggregate
      ↓
TakeOrderedAndProject
      ↓
Top 100
```

The important question is whether Spark can avoid the second global ordering shuffle and full sort.

---

# 3. Dataset

The experiment uses deterministic synthetic transaction data.

| Parameter                     |      Value |
| ----------------------------- | ---------: |
| Transactions                  | 10,000,000 |
| Customers                     |  1,000,000 |
| Configured shuffle partitions |        200 |
| Top-N                         |        100 |
| Input partitions              |          2 |
| AQE                           |    Enabled |
| AQE Coalescing                |    Enabled |

Input generation:

```scala
val transactions =
  spark.range(0, transactionCount)
    .selectExpr(
      "id AS transaction_id",
      s"id % $customerCount AS customer_id",
      "id % 1000 AS amount"
    )
```

The deterministic data generation makes the experiment reproducible.

---

# 4. Execution Architecture

## Run 1 — Full ORDER BY

```text
                    10M transactions
                           │
                           ▼
                       Project
                           │
                           ▼
                  Partial HashAggregate
                           │
                           ▼
                 Shuffle by customer_id
                    200 partitions
                           │
                           ▼
                   Final HashAggregate
                           │
                           ▼
                1M customer records
                           │
                           ▼
              Range Partitioning Shuffle
                           │
                           ▼
                         Sort
                           │
                           ▼
                 1M globally ordered rows
```

---

## Run 2 — Top-N

```text
                    10M transactions
                           │
                           ▼
                       Project
                           │
                           ▼
                  Partial HashAggregate
                           │
                           ▼
                 Shuffle by customer_id
                    200 partitions
                           │
                           ▼
                   Final HashAggregate
                           │
                           ▼
                1M customer records
                           │
                           ▼
              TakeOrderedAndProject
                           │
                           ▼
                       Top 100
```

The second ordering shuffle and full global Sort disappear.

---

# 5. Run 1 — Full Global ORDER BY

## Query

```scala
transactions
  .groupBy("customer_id")
  .sum("amount")
  .orderBy(desc("sum(amount)"))
```

## Observed Runtime

```text
Query duration: ~4 seconds
```

## Observed Output

```text
Aggregation output: 1,000,000 rows
Final output:       1,000,000 rows
```

---

# 6. Run 1 — Shuffle Metrics

### First aggregation shuffle

```text
Shuffle records written: 2,000,000
Shuffle bytes written:   18.7 MiB
Configured partitions:   200
```

This is the shuffle required to bring records belonging to the same `customer_id` together.

### Second ordering shuffle

```text
Shuffle records written: 1,000,000
Shuffle bytes written:   8.0 MiB
```

This second shuffle is required because Spark must establish global ordering by:

```text
sum(amount) DESC
```

Therefore approximate total shuffle output is:

```text
18.7 MiB + 8.0 MiB
≈ 26.7 MiB
```

---

# 7. Run 1 — Physical Plan

The final executed plan was:

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

The critical operator is:

```text
Exchange (8)
Arguments:
rangepartitioning(sum(amount) DESC NULLS LAST, 200)
```

followed by:

```text
Sort (11)
Arguments:
[sum(amount) DESC NULLS LAST]
```

This proves that the full `ORDER BY` requires a global ordering stage.

---

# 8. Run 1 — Execution Interpretation

The execution can be simplified to:

```text
10M input rows
      ↓
Partial aggregation
      ↓
2M records
      ↓
Customer shuffle
      ↓
1M customer aggregates
      ↓
Range-partitioning shuffle
      ↓
Global Sort
      ↓
1M ordered rows
```

The important observation is that Spark must process the entire 1M-row aggregate result for global ordering.

---

# 9. Run 2 — Top-N

## Query

```scala
transactions
  .groupBy("customer_id")
  .sum("amount")
  .orderBy(desc("sum(amount)"))
  .limit(100)
```

## Observed Runtime

```text
Query duration: ~2 seconds
```

Compared with Run 1:

```text
Run 1: ~4 seconds
Run 2: ~2 seconds
```

Observed improvement:

```text
~50% lower query duration
```

This is an observation from this local benchmark, not a universal performance guarantee.

---

# 10. Run 2 — Output Metrics

The final aggregation still produces:

```text
1,000,000 rows
```

because Spark still has to calculate the aggregate for every customer.

However, the final Top-N result contains only:

```text
100 rows
```

This distinction is important:

```text
Aggregation cardinality = 1,000,000
Final result cardinality = 100
```

The Top-N optimization does not eliminate the aggregation.

It optimizes the **ordering stage after aggregation**.

---

# 11. Run 2 — Shuffle Metrics

The aggregation shuffle remains:

```text
Shuffle records written: 2,000,000
Shuffle bytes written:   18.7 MiB
```

This shuffle is unavoidable for the `GROUP BY customer_id` operation under this execution strategy.

However, the second ordering shuffle disappears.

There is no:

```text
1,000,000-row range-partitioning shuffle
```

and no corresponding:

```text
8.0 MiB ordering shuffle
```

Therefore:

```text
Run 1:
18.7 MiB + 8.0 MiB
≈ 26.7 MiB

Run 2:
18.7 MiB
```

The Top-N query therefore reduced observed shuffle bytes by approximately:

```text
(26.7 - 18.7) / 26.7
≈ 30%
```

More importantly, an entire global ordering Exchange is eliminated.

---

# 12. Run 2 — Top-N Shuffle Evidence

The `TakeOrderedAndProject` execution reported:

```text
Shuffle records written: 200
Records read:            200
Shuffle bytes written:   1,414 bytes
```

This is dramatically smaller than the 1M-record ordering shuffle used by the full `ORDER BY`.

Conceptually:

```text
FULL ORDER BY

1,000,000 aggregate rows
        ↓
1,000,000-row ordering shuffle
        ↓
Global Sort
        ↓
1,000,000 ordered rows
```

versus:

```text
TOP-N

1,000,000 aggregate rows
        ↓
Local Top-N candidate processing
        ↓
Small candidate exchange/merge
        ↓
Top 100
```

---

# 13. Run 2 — Physical Plan

The final executed plan was:

```text
== Physical Plan ==
AdaptiveSparkPlan (14)
+- == Final Plan ==
   DeserializeToObject (9)
   +- TakeOrderedAndProject (8)
      +- * HashAggregate (7)
         +- AQEShuffleRead (6)
            +- ShuffleQueryStage (5), Statistics(sizeInBytes=45.8 MiB, rowCount=2.00E+6)
               +- Exchange (4)
                  +- * HashAggregate (3)
                     +- * Project (2)
                        +- * Range (1)
```

The critical operator is:

```text
TakeOrderedAndProject
Arguments:
100, [sum(amount) DESC NULLS LAST], [customer_id, sum(amount)]
```

This is the strongest evidence from the experiment.

---

# 14. What Disappeared?

Compare the two final plans.

## Full ORDER BY

```text
HashAggregate
      ↓
AQEShuffleRead
      ↓
Exchange
      ↓
Sort
```

## Top-N

```text
HashAggregate
      ↓
AQEShuffleRead
      ↓
TakeOrderedAndProject
```

The following operators disappeared:

```text
Exchange
  rangepartitioning(...)
```

and:

```text
Sort
```

This is the core optimization demonstrated by Module 1.7.8.

---

# 15. Why TakeOrderedAndProject Is Better

Suppose Spark has:

```text
1,000,000 customer aggregates
```

and the query asks for:

```text
Top 100
```

A full global sort conceptually requires Spark to establish ordering across the entire dataset.

But Top-N only requires identifying:

```text
100 best candidates
```

Spark can therefore maintain bounded Top-N candidates rather than constructing a fully globally sorted 1M-row dataset.

The specialized operator:

```text
TakeOrderedAndProject
```

is designed for this pattern.

Conceptually:

```text
ORDER BY
────────

Sort all 1M rows
        ↓
Return all 1M rows
```

versus:

```text
ORDER BY + LIMIT 100
────────────────────

Maintain Top 100 candidates
        ↓
Merge candidates
        ↓
Return 100 rows
```

---

# 16. AQE Behavior

Both runs used Adaptive Query Execution.

The aggregation Exchange was configured with:

```text
200 partitions
```

AQE subsequently coalesced the shuffle read to:

```text
2 partitions
```

Run 2 reported:

```text
AQEShuffleRead
number of partitions: 2
number of coalesced partitions: 2
Arguments: coalesced
```

Therefore the execution combines two optimization mechanisms:

```text
AQE
 │
 └── Coalesce 200 configured partitions
             ↓
          2 partitions
```

and:

```text
ORDER BY + LIMIT N
 │
 └── TakeOrderedAndProject
             ↓
      Avoid global Sort
```

These optimizations solve different problems.

---

# 17. Aggregation Still Requires a Shuffle

An important engineering lesson is that Top-N does **not** remove the aggregation shuffle.

The plan still contains:

```text
Exchange
Arguments:
hashpartitioning(customer_id, 200)
```

Why?

Because Spark needs to combine all transactions belonging to the same customer.

The execution remains:

```text
10M transactions
      ↓
Partial HashAggregate
      ↓
Customer shuffle
      ↓
Final HashAggregate
      ↓
1M customer aggregates
```

Only the subsequent ordering operation is optimized.

---

# 18. Memory and Spill Metrics

## Run 2

First HashAggregate:

```text
Peak memory: 192 MiB
Spill:       0 B
```

Final HashAggregate:

```text
Peak memory: 160 MiB
Spill:       0 B
```

Sort fallback:

```text
0 tasks
```

There was no significant spill pressure in this benchmark.

This is expected because the experiment is running on a controlled local dataset and does not intentionally create memory pressure.

---

# 19. Run 1 vs Run 2 — Final Comparison

| Metric                        | Full ORDER BY |        Top-N |
| ----------------------------- | ------------: | -----------: |
| Input rows                    |           10M |          10M |
| Customers                     |            1M |           1M |
| Configured shuffle partitions |           200 |          200 |
| Aggregation output            |            1M |           1M |
| Final output                  |            1M |      **100** |
| Query duration                |          ~4 s |     **~2 s** |
| First shuffle records         |            2M |           2M |
| First shuffle bytes           |      18.7 MiB |     18.7 MiB |
| Second ordering shuffle       |       **Yes** |       **No** |
| Ordering shuffle records      |            1M |     **None** |
| Ordering shuffle bytes        |       8.0 MiB |     **None** |
| Global Sort                   |       **Yes** |       **No** |
| `TakeOrderedAndProject`       |            No |      **Yes** |
| AQE coalescing                |  2 partitions | 2 partitions |
| Spill                         |          None |         None |
| Sort fallback                 |          None |         None |

---

# 20. Performance Improvement

Observed:

```text
Full ORDER BY:       ~4 s
Top-N:               ~2 s
```

Approximate runtime reduction:

```text
(4 - 2) / 4 × 100
≈ 50%
```

Observed shuffle reduction:

```text
Full ORDER BY:
≈ 26.7 MiB

Top-N:
≈ 18.7 MiB
```

Approximate reduction:

```text
≈ 30%
```

The more significant architectural improvement is the removal of the entire:

```text
Range Partitioning Exchange
        +
Global Sort
```

stage.

---

# 21. Spark Execution Insight

The experiment demonstrates an important distinction:

### Full global ordering

```text
ORDER BY metric DESC
```

requires Spark to establish global ordering.

This can involve:

```text
RangePartitioning
      ↓
Shuffle
      ↓
Sort
```

### Top-N ordering

```text
ORDER BY metric DESC
LIMIT N
```

provides Spark with an upper bound on the required result size.

Spark can exploit that requirement using:

```text
TakeOrderedAndProject
```

and avoid the full global sorting strategy.

---

# 22. Production Engineering Guidance

## Prefer Top-N when the business only needs Top-N

Instead of:

```sql
SELECT customer_id, SUM(amount) AS total_amount
FROM transactions
GROUP BY customer_id
ORDER BY total_amount DESC;
```

when the consumer only needs the first 100 rows, use:

```sql
SELECT customer_id, SUM(amount) AS total_amount
FROM transactions
GROUP BY customer_id
ORDER BY total_amount DESC
LIMIT 100;
```

The second query communicates the actual business requirement to Spark.

That gives the optimizer an opportunity to select a specialized Top-N execution strategy.

---

# 23. When Full ORDER BY Is Still Required

Do not blindly add `LIMIT`.

If downstream processing genuinely requires:

```text
all 1M customers globally ordered
```

then:

```sql
ORDER BY
```

is necessary.

For example:

* exporting every customer in ranking order
* generating a complete ranked report
* downstream processing that consumes the complete ordered dataset

In these situations the global ordering cost is part of the actual requirement.

---

# 24. Important Trade-Off

Top-N is most useful when:

```text
N << total result rows
```

For example:

```text
1,000,000 rows → Top 10
1,000,000 rows → Top 100
1,000,000 rows → Top 1,000
```

The optimization becomes less compelling as `N` approaches the full result size.

Conceptually:

```text
N = 10
     ↓
Very small result

N = 100
     ↓
Excellent Top-N use case

N = 100,000
     ↓
Still potentially useful

N = 900,000
     ↓
Much less benefit

N = 1,000,000
     ↓
Essentially the complete result
```

Therefore the business requirement should drive the choice.

---

# 25. Common Anti-Pattern

A common application-level anti-pattern is:

```text
GROUP BY
   ↓
Collect huge result to application
   ↓
Sort in application
   ↓
Take first N
```

This is undesirable because it:

* moves large amounts of data out of Spark
* increases driver/application memory pressure
* wastes distributed processing
* prevents Spark from optimizing the query
* can create scalability bottlenecks

Prefer expressing the requirement directly:

```text
ORDER BY
LIMIT N
```

inside Spark SQL/DataFrame operations.

---

# 26. Key Spark Internals

The important physical operators from this experiment are:

### `HashAggregate`

Performs partial and final aggregation.

```text
Partial HashAggregate
        ↓
Shuffle
        ↓
Final HashAggregate
```

### `Exchange`

Introduces data redistribution.

For aggregation:

```text
hashpartitioning(customer_id, 200)
```

For global ordering in Run 1:

```text
rangepartitioning(sum(amount) DESC, 200)
```

### `Sort`

Used by the full global ordering strategy.

### `AQEShuffleRead`

Represents adaptive reading of shuffle output.

In this experiment:

```text
200 configured partitions
        ↓
AQE
        ↓
2 coalesced partitions
```

### `TakeOrderedAndProject`

Specialized Top-N operator.

In this experiment:

```text
TakeOrderedAndProject
Arguments:
100, [sum(amount) DESC NULLS LAST]
```

This operator is the defining physical-plan evidence for the Top-N optimization.

---

# 27. Final Physical Plan Comparison

## Full ORDER BY

```text
AdaptiveSparkPlan
+- Final Plan
   +- Sort
      +- AQEShuffleRead
         +- ShuffleQueryStage
            +- Exchange
               rangepartitioning(sum(amount) DESC, 200)
               +- HashAggregate
                  +- AQEShuffleRead
                     +- ShuffleQueryStage
                        +- Exchange
                           hashpartitioning(customer_id, 200)
                           +- HashAggregate
                              +- Project
                                 +- Range
```

---

## Top-N

```text
AdaptiveSparkPlan
+- Final Plan
   +- TakeOrderedAndProject
      +- HashAggregate
         +- AQEShuffleRead
            +- ShuffleQueryStage
               +- Exchange
                  hashpartitioning(customer_id, 200)
                  +- HashAggregate
                     +- Project
                        +- Range
```

The visual difference is the essence of this module:

```text
FULL ORDER BY

Aggregation
    ↓
Shuffle
    ↓
Aggregation
    ↓
Shuffle
    ↓
Sort
    ↓
All rows


TOP-N

Aggregation
    ↓
Shuffle
    ↓
Aggregation
    ↓
TakeOrderedAndProject
    ↓
Top N
```

---

# 28. Experiment Validation Checklist

| Validation                               | Result |
| ---------------------------------------- | ------ |
| Aggregation shuffle present              | ✅      |
| 200 configured shuffle partitions        | ✅      |
| AQE enabled                              | ✅      |
| AQE coalescing observed                  | ✅      |
| Full ORDER BY creates second shuffle     | ✅      |
| Full ORDER BY creates Sort               | ✅      |
| Top-N eliminates second ordering shuffle | ✅      |
| Top-N eliminates global Sort             | ✅      |
| `TakeOrderedAndProject` observed         | ✅      |
| Final output limited to 100              | ✅      |
| No aggregation spill                     | ✅      |
| No sort fallback                         | ✅      |
| Runtime improvement observed             | ✅      |

---

# 29. Interview Questions

## Q1. Why is `ORDER BY` expensive in Spark?

Because global ordering requires data to be redistributed so that Spark can establish a globally ordered result.

This can introduce:

```text
Exchange
+
Sort
```

---

## Q2. Why can `ORDER BY ... LIMIT N` be cheaper?

Because Spark knows only the best `N` rows are required and can use a specialized Top-N strategy instead of globally sorting the entire result.

---

## Q3. Which physical operator demonstrates the Top-N optimization?

```text
TakeOrderedAndProject
```

---

## Q4. Does Top-N eliminate the aggregation shuffle?

No.

The aggregation still requires:

```text
hashpartitioning(customer_id)
```

in this execution.

---

## Q5. What disappeared in the Top-N physical plan?

The full ordering:

```text
Exchange
rangepartitioning(...)
```

and:

```text
Sort
```

were replaced by:

```text
TakeOrderedAndProject
```

---

## Q6. What is the difference between `Sort` and `TakeOrderedAndProject`?

`Sort` is used to establish ordering across the relevant dataset.

`TakeOrderedAndProject` is specialized for returning only the top/bottom N rows according to an ordering.

---

## Q7. Why is the aggregation output still 1M rows even though LIMIT is 100?

Because there are still 1M distinct customers that must be aggregated.

The `LIMIT` reduces the final result, not the number of groups that need to be computed.

---

## Q8. What role does AQE play here?

AQE dynamically coalesces shuffle partitions.

In this experiment:

```text
200 configured
      ↓
2 coalesced
```

This is independent of the Top-N optimization.

---

# 30. Production Optimization Checklist

When reviewing a Spark query that ranks or sorts data:

```text
1. What is the required result cardinality?
2. Does the consumer really need all rows?
3. Can the query use LIMIT N?
4. Does the physical plan contain Sort?
5. Does it contain rangepartitioning?
6. Does Spark use TakeOrderedAndProject?
7. Is AQE enabled?
8. Are shuffle partitions appropriately configured?
9. Is N significantly smaller than the total result?
10. Are results unnecessarily collected to the driver?
```

The goal is to make the business requirement explicit in the Spark query.

---

# 31. Enterprise Data Engineering Takeaway

The key principle from this experiment is:

> **Don't globally sort data when the business only needs Top-N.**

For a large-scale dataset:

```text
GROUP BY
+
ORDER BY
```

can require:

```text
Aggregation Shuffle
+
Ordering Shuffle
+
Global Sort
```

while:

```text
GROUP BY
+
ORDER BY
+
LIMIT N
```

can allow Spark to use:

```text
Aggregation Shuffle
+
TakeOrderedAndProject
```

The difference becomes increasingly important as the number of aggregated rows grows.

---

# 32. Final Conclusion

Module 1.7.8 successfully demonstrated that Spark can optimize:

```text
GROUP BY
ORDER BY
LIMIT N
```

using:

```text
TakeOrderedAndProject
```

instead of performing a full global sort.

The actual benchmark showed:

```text
Full ORDER BY
~4 seconds
~26.7 MiB observed shuffle output
Range-partitioning shuffle
Global Sort
1M final rows
```

versus:

```text
Top-N
~2 seconds
~18.7 MiB aggregation shuffle
No second ordering shuffle
No global Sort
TakeOrderedAndProject
100 final rows
```

The measured local benchmark therefore showed approximately:

```text
50% lower query duration
~30% lower shuffle bytes
```

while eliminating an entire global ordering stage.

The most important physical-plan evidence is:

```text
TakeOrderedAndProject
Arguments:
100, [sum(amount) DESC NULLS LAST]
```

This demonstrates that the optimizer is not merely reducing the final output size—it is selecting a fundamentally different execution strategy for the Top-N requirement.

---

# 33. Module 1.7.8 — What We Learned

```text
GROUP BY
    ↓
Aggregation requires shuffle
```

```text
GROUP BY + ORDER BY
    ↓
Aggregation shuffle
    +
Global ordering shuffle
    +
Sort
```

```text
GROUP BY + ORDER BY + LIMIT N
    ↓
Aggregation shuffle
    +
TakeOrderedAndProject
```

### Core principle

```text
Full ordering ≠ Top-N
```

When the business requirement is Top-N, explicitly express:

```text
ORDER BY ... LIMIT N
```

and allow Spark's optimizer to choose the specialized execution path.

---

## Module Status

**Completed**

```text
Module 1.7.8 — Top-N Aggregation
```

Evidence captured:

* ✅ Baseline full global ORDER BY
* ✅ Top-N execution
* ✅ Runtime comparison
* ✅ Shuffle comparison
* ✅ AQE behavior
* ✅ Physical-plan comparison
* ✅ `TakeOrderedAndProject` confirmation
* ✅ Global Sort elimination
* ✅ Ordering shuffle elimination
* ✅ Memory and spill analysis
* ✅ Production engineering guidance
* ✅ Interview-level Spark internals
