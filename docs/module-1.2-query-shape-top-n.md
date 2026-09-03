# Spark Performance: Why `ORDER BY ... LIMIT 100` Can Be Much Cheaper Than `ORDER BY`

A common Spark optimization mistake is to look at SQL syntax and assume that small changes in the query produce proportionally small changes in execution.

They don't always.

Consider these two queries:

```sql
SELECT customer_id, SUM(amount) AS total_amount
FROM transactions
GROUP BY customer_id
ORDER BY total_amount DESC;
```

and:

```sql
SELECT customer_id, SUM(amount) AS total_amount
FROM transactions
GROUP BY customer_id
ORDER BY total_amount DESC
LIMIT 100;
```

The second query differs by only one clause.

But the physical execution strategy can be fundamentally different.

I built a deterministic Spark benchmark to understand exactly what happens.

---

## The Use Case

Imagine a transaction-processing platform with millions of transactions and hundreds of thousands or millions of customers.

The platform calculates customer-level transaction totals.

There are two consumers:

### Consumer 1 — Complete ranking

An analytics process needs:

> Every customer, ordered from highest transaction value to lowest.

### Consumer 2 — Dashboard

A dashboard needs:

> Only the top 100 customers.

A naive implementation might assume that Spark simply performs the same sort and then throws away everything after row 100.

That would be unnecessarily expensive.

The interesting question is:

> **Does Spark actually recognize the Top-N requirement and change the physical execution strategy?**

---

# The Benchmark

I created a deterministic dataset containing:

* 10 million transactions
* 1 million customers
* 6 initial input partitions
* 64 configured shuffle partitions
* Adaptive Query Execution enabled
* cached input
* `local[*]` execution

The dataset contains:

```text
transaction_id
customer_id
city
amount
```

The values are generated deterministically from the transaction ID.

That matters because benchmark data should be reproducible. If the input changes between runs, it becomes difficult to distinguish query behavior from data-distribution effects.

---

# Why Cache the Dataset?

The benchmark materializes the dataset before running the scenarios:

```scala
val transactions = spark.range(rowCount)
  ...
  .cache()

transactions.count()
```

This separates dataset generation/materialization from the actual query comparison.

Without this step, we risk measuring:

```text
Data generation
+ cache population
+ query execution
```

instead of primarily measuring:

```text
Query execution
```

The cache also allows the scenarios to operate against the same materialized input.

---

# Scenario A — GROUP BY + ORDER BY

The first workload is:

```scala
val baseline = transactions
  .groupBy("customer_id")
  .agg(
    sum("amount").alias("total_amount"),
    count("*").alias("transaction_count")
  )
  .orderBy(desc("total_amount"))
```

Conceptually, Spark needs to perform:

```text
10M transactions
       |
       v
Partial aggregation
       |
       v
Hash shuffle by customer_id
       |
       v
Final aggregation
       |
       v
Global ordering
       |
       v
1M ordered customers
```

The physical plan showed the important sequence:

```text
Partial HashAggregate
        |
        v
Exchange
hashpartitioning(customer_id)
        |
        v
Final HashAggregate
        |
        v
Exchange
rangepartitioning(total_amount DESC)
        |
        v
Sort
```

There are two fundamentally different shuffle operations.

---

# Shuffle #1 — Aggregation

Spark cannot calculate a customer's final total independently on every partition.

Transactions for the same customer may exist on different input partitions.

Therefore, Spark has to redistribute data:

```text
Partition 0 ─┐
Partition 1 ─┤
Partition 2 ─┤──> hash(customer_id) ──> customer partition
Partition 3 ─┤
Partition 4 ─┤
Partition 5 ─┘
```

This allows all partial values for a customer to reach the same downstream aggregation task.

This shuffle is fundamentally required by the distributed aggregation.

---

# Shuffle #2 — Global Ordering

After aggregation, Spark has approximately 1 million customer-level results.

Now the query says:

```text
ORDER BY total_amount DESC
```

That is a global requirement.

It isn't enough for every partition to be locally sorted.

For example:

```text
Partition 1:
900
800
700

Partition 2:
950
850
750
```

Each partition may be locally sorted, but the combined dataset isn't globally ordered.

Spark therefore needs another mechanism to establish global ordering.

The physical plan contains:

```text
Exchange
rangepartitioning(total_amount DESC)
        |
        v
Sort
```

This is additional distributed work.

---

# Scenario B — GROUP BY Only

Now remove the ordering:

```scala
val aggregationOnly = transactions
  .groupBy("customer_id")
  .agg(
    sum("amount").alias("total_amount"),
    count("*").alias("transaction_count")
  )
```

The global ordering requirement disappears.

The execution is essentially:

```text
Partial HashAggregate
        |
        v
Exchange
hashpartitioning(customer_id)
        |
        v
Final HashAggregate
```

There is no reason for Spark to globally order the million customer results.

This is an important performance principle:

> **The cost of a distributed query is strongly influenced by the semantics of the requested result, not simply the amount of input data.**

---

# Scenario C — The Interesting One

Now add:

```scala
.limit(100)
```

The query becomes:

```scala
val topN = transactions
  .groupBy("customer_id")
  .agg(
    sum("amount").alias("total_amount"),
    count("*").alias("transaction_count")
  )
  .orderBy(desc("total_amount"))
  .limit(100)
```

At first glance, it might seem that Spark has to:

```text
Aggregate 1M customers
        ↓
Sort 1M customers
        ↓
Return first 100
```

But that's not what the physical plan shows.

Instead:

```text
Partial HashAggregate
        |
        v
Exchange
hashpartitioning(customer_id)
        |
        v
Final HashAggregate
        |
        v
TakeOrderedAndProject(100)
```

The second range-partitioning exchange is gone.

The separate global sort is gone.

That's the important observation.

---

# `TakeOrderedAndProject`

Spark recognizes that the query doesn't require a complete globally ordered dataset.

It only needs the best 100 rows according to the ordering expression.

The physical operator becomes:

```text
TakeOrderedAndProject(100)
```

Conceptually, this is a Top-N problem.

Instead of asking:

> "How do I globally sort all one million customers?"

Spark can ask:

> "Which 100 customers belong in the final result?"

That is a much smaller problem.

This is one of the most useful reasons to inspect physical plans rather than reasoning only from SQL syntax.

---

# What the Spark UI Showed

The Scenario C aggregation stage had six completed tasks.

The task-level metrics were remarkably consistent:

| Metric          |        Observation |
| --------------- | -----------------: |
| Duration        |        0.9–1.0 sec |
| Median          |            0.9 sec |
| GC              |          5 ms/task |
| Input           | 33.3–33.4 MiB/task |
| Shuffle write   |     ~10.7 MiB/task |
| Shuffle records |            1M/task |
| Locality        |      Process local |
| Failures        |                  0 |

This tells us several things.

## There is no meaningful skew

Every task processed almost exactly the same amount of input.

The shuffle output was also almost identical.

There is no obvious task behaving like:

```text
Task 1 → 0.9 sec
Task 2 → 0.9 sec
Task 3 → 0.9 sec
Task 4 → 0.9 sec
Task 5 → 0.9 sec
Task 6 → 8.0 sec
```

That kind of distribution would immediately suggest a straggler or skew problem.

We don't see that here.

---

# GC Is Not the Bottleneck

Each task spent approximately:

```text
5 ms
```

in GC.

Against approximately:

```text
0.9–1.0 seconds
```

of execution time, that's negligible.

So this benchmark isn't demonstrating a GC-bound workload.

That's important because optimization should follow evidence.

There would be little value in changing JVM memory settings based on this run.

---

# What Happened to the 64 Shuffle Partitions?

The benchmark configured:

```scala
spark.sql.shuffle.partitions = 64
```

But Adaptive Query Execution is enabled:

```scala
.config("spark.sql.adaptive.enabled", "true")
```

The runtime statistics allow Spark to coalesce small shuffle partitions.

In the Scenario C execution, AQE reduced the downstream shuffle workload to a much smaller number of effective partitions.

This illustrates another important Spark principle:

> Configured shuffle partitions are not necessarily the same thing as the number of tasks that ultimately execute.

AQE can make runtime decisions using information unavailable during static planning.

---

# The Most Important Comparison

The physical plans can be simplified to:

### Full ordering

```text
Aggregate
   ↓
Hash Shuffle
   ↓
Aggregate
   ↓
Range Shuffle
   ↓
Sort
   ↓
1M rows
```

### Top 100

```text
Aggregate
   ↓
Hash Shuffle
   ↓
Aggregate
   ↓
TakeOrderedAndProject(100)
   ↓
100 rows
```

That is much more significant than:

> "The second query returns fewer rows."

The query has changed the computational problem.

---

# A Common Misunderstanding About `LIMIT`

It's tempting to think of `LIMIT` as merely a result-size filter:

```text
Do all the expensive work
        ↓
Throw away most of the result
```

That is not necessarily how Spark executes a Top-N query.

When the ordering and limit can be combined into a Top-N strategy, Spark can avoid a full global ordering.

This is why understanding the physical plan matters.

---

# Is the First Shuffle Still Expensive?

Yes.

Adding `LIMIT 100` does **not** eliminate the aggregation shuffle.

Spark still has to determine the total transaction amount for every customer before it can know which customers belong in the top 100.

In other words, Spark cannot simply inspect 100 arbitrary input rows and declare them the top customers.

It still needs the customer-level aggregates.

Therefore:

```text
10M transactions
        ↓
customer aggregation
        ↓
1M customer aggregates
        ↓
Top 100
```

The optimization primarily affects the **post-aggregation ordering phase**.

That distinction is important.

---

# A Shuffle Is Not Automatically Bad

This benchmark also reinforces something I consider fundamental to Spark performance engineering:

> **Don't optimize away a shuffle simply because it is a shuffle.**

Ask what the shuffle is accomplishing.

The aggregation shuffle:

```text
hashpartition(customer_id)
```

is necessary to correctly combine distributed customer records.

The global ordering shuffle:

```text
rangepartition(total_amount)
```

is necessary when the requirement is to globally order all results.

But when the requirement changes to:

```text
top 100
```

the second operation becomes unnecessary.

The correct optimization isn't:

> "Remove all shuffles."

It is:

> **"Eliminate work that the business requirement doesn't actually require."**

---

# What I Would Check in Production

If I encountered a similar Spark workload in production, I would not immediately change:

* executor memory
* number of cores
* JVM options
* shuffle partitions
* serialization settings

I would first inspect:

### 1. Physical plan

Look for:

```text
Exchange
Sort
BroadcastHashJoin
SortMergeJoin
HashAggregate
TakeOrderedAndProject
```

### 2. Shuffle metrics

Check:

* bytes written
* records written
* bytes read
* partition distribution

### 3. Task distribution

Look for:

* stragglers
* skew
* uneven input
* uneven shuffle output

### 4. Spill

Check whether:

* memory spill
* disk spill
* sort spill

is occurring.

### 5. GC

If GC is negligible, don't waste time optimizing GC.

### 6. AQE behavior

Understand whether runtime partition coalescing or skew handling is changing the original plan.

---

# Benchmark Caveat

This benchmark runs with:

```scala
.master("local[*]")
```

Therefore, the absolute execution times are not production-cluster benchmarks.

For example:

```text
Scenario A = X seconds
Scenario C = Y seconds
```

should not be interpreted as:

> "A production Spark cluster will always be X/Y times faster."

Hardware, executor configuration, network bandwidth, storage, serialization, concurrency, data distribution and cluster topology can all change the absolute numbers.

The more valuable evidence is the **physical-plan difference**.

The benchmark demonstrates a repeatable execution behavior:

```text
ORDER BY
→ global ordering strategy

ORDER BY + LIMIT 100
→ Top-N strategy
```

---

# The Engineering Lesson

The biggest Spark performance gains don't always come from tuning Spark configuration.

Sometimes they come from changing the computation itself.

A useful optimization mindset is:

```text
Business requirement
        ↓
What result is actually required?
        ↓
What computation is mathematically necessary?
        ↓
What physical plan does Spark generate?
        ↓
Which operations dominate the cost?
        ↓
Can unnecessary work be eliminated?
```

In this benchmark, the key optimization isn't:

> "Increase executors."

It isn't:

> "Increase shuffle partitions."

It isn't:

> "Give Spark more memory."

It is simply recognizing:

> **If the consumer only needs the top 100 results, globally sorting all 1 million results is unnecessary work.**

Spark's `TakeOrderedAndProject` physical strategy reflects that distinction.

---

# Final Takeaway

When reviewing Spark SQL performance, don't stop at the logical query.

Ask three questions:

### 1. What does the query require?

Aggregation?

Join?

Global ordering?

Top-N?

### 2. What physical work does Spark choose?

Look for:

```text
Exchange
HashAggregate
Sort
TakeOrderedAndProject
```

### 3. Is all of that work actually necessary?

That final question is where meaningful performance engineering begins.

A one-line SQL change can sometimes eliminate an entire distributed execution phase.

And that is why reading Spark's physical plan is often more valuable than simply looking at the query runtime.
