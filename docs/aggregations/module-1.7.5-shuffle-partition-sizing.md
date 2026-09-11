Absolutely. Here is **Module 1.7.5 — Sort-Based Aggregation** in the same copy-paste-ready format as the previous 1.7 modules.

# Module 1.7.5 — Sort-Based Aggregation

## Experiment Objective

Understand how Apache Spark can execute an aggregation using a **sort-based aggregation strategy**, and how this differs from hash-based aggregation.

The aggregation being investigated is:

```text
SUM(amount) GROUP BY customer_id
```

The experiment focuses on:

* Sort-based aggregation
* `SortAggregateExec`
* `HashAggregateExec` vs `SortAggregateExec`
* Why Spark normally prefers hash aggregation
* Conditions under which Spark may fall back to sort-based aggregation
* Impact of sorting on CPU and shuffle performance
* Reading the physical execution plan
* Connecting Spark SQL plans with actual execution behavior

---

# 1. Business Use Case

Assume we have a large transaction dataset:

```text
transaction_id
customer_id
amount
transaction_date
```

We want to calculate the total transaction amount for every customer:

```sql
SELECT
    customer_id,
    SUM(amount) AS total_amount
FROM transactions
GROUP BY customer_id;
```

Conceptually:

```text
Transactions
     |
     v
Group by customer_id
     |
     v
SUM(amount)
     |
     v
Customer totals
```

Example input:

```text
customer_id | amount
------------|-------
101         | 100
102         | 250
101         | 300
103         | 150
102         | 100
```

Expected output:

```text
customer_id | total_amount
------------|------------
101         | 400
102         | 350
103         | 150
```

---

# 2. What Is Sort-Based Aggregation?

In a sort-based aggregation strategy, Spark first ensures that rows are ordered by the grouping key.

For:

```text
GROUP BY customer_id
```

the data is conceptually arranged as:

```text
customer_id | amount
------------|-------
101         | 100
101         | 300
102         | 250
102         | 100
103         | 150
```

Spark can then process consecutive rows belonging to the same customer.

Conceptually:

```text
101 -> 100 + 300 = 400
102 -> 250 + 100 = 350
103 -> 150       = 150
```

The important idea is:

> Sorting makes all records belonging to the same grouping key adjacent.

This allows Spark to aggregate records by scanning the sorted data.

---

# 3. Sort-Based Aggregation vs Hash Aggregation

There are two important aggregation approaches to understand.

## Hash Aggregation

Hash aggregation maintains an in-memory hash structure:

```text
customer_id -> running total
```

Example:

```text
101 -> 400
102 -> 350
103 -> 150
```

As rows arrive:

```text
row customer_id=101
        |
        v
lookup 101
        |
        v
update running total
```

Conceptually:

```text
Input
  |
  v
Hash map
  |
  +--> 101 -> 400
  +--> 102 -> 350
  +--> 103 -> 150
```

---

## Sort-Based Aggregation

Sort-based aggregation works differently:

```text
Input
  |
  v
Sort by customer_id
  |
  v
Sequential scan
  |
  v
Aggregate consecutive keys
```

Example:

```text
101 -> 100
101 -> 300
102 -> 250
102 -> 100
103 -> 150
```

Spark can process:

```text
101: 100 + 300
102: 250 + 100
103: 150
```

---

# 4. Why Would Spark Use Sort-Based Aggregation?

Hash aggregation is generally very efficient, so Spark prefers it for many common aggregation workloads.

However, hash aggregation has an important requirement:

```text
Aggregation state must be maintained in memory.
```

If the aggregation state becomes expensive or the aggregation cannot be efficiently represented using Spark's normal hash aggregation implementation, Spark can use a sort-based strategy.

The general trade-off is:

```text
Hash aggregation
    |
    +--> Fast lookups
    +--> No explicit sort required
    +--> Memory required for hash map
```

versus:

```text
Sort aggregation
    |
    +--> Requires sorting
    +--> Sequential processing
    +--> Can avoid maintaining a large hash map
```

Therefore:

> Sort-based aggregation trades additional sorting work for a different memory/execution strategy.

---

# 5. Important Spark Physical Operators

When examining Spark SQL execution plans, two operators are particularly important:

```text
HashAggregateExec
```

and:

```text
SortAggregateExec
```

A typical hash aggregation plan may contain:

```text
HashAggregate
  |
  +-- Exchange
        |
        +-- HashAggregate
```

A sort-based aggregation plan may look conceptually like:

```text
SortAggregate
  |
  +-- Sort
        |
        +-- Exchange
              |
              +-- SortAggregate
```

The exact physical plan depends on Spark version, query, configuration, data types, and optimizer decisions.

---

# 6. Experiment Setup

Use the same dataset and benchmark configuration as the previous aggregation experiments.

Recommended baseline:

```text
Rows              = 10,000,000
Customers         = 1,000,000
Shuffle partitions = 64
```

The important requirement is that the input data and benchmark conditions remain consistent.

This allows us to compare:

```text
1.7.1 Baseline Aggregation
1.7.2 Hash Aggregation
1.7.3 Partial Aggregation
1.7.4 Aggregation + Ordering
1.7.5 Sort-Based Aggregation
```

without changing the workload itself.

---

# 7. Query

Use:

```scala
val result = transactions
  .groupBy("customer_id")
  .agg(sum("amount").alias("total_amount"))
```

Equivalent SQL:

```sql
SELECT
    customer_id,
    SUM(amount) AS total_amount
FROM transactions
GROUP BY customer_id;
```

---

# 8. Forcing / Encouraging Sort-Based Aggregation

Spark normally decides the physical aggregation strategy automatically.

For an experiment specifically studying `SortAggregateExec`, the objective is to make Spark choose a sort-based strategy where possible.

One useful experimental approach is to use an aggregation expression that is not efficiently handled by the normal hash aggregation path, or to construct a query/configuration where Spark falls back to sort aggregation.

The important point is:

> Do not interpret a configuration switch as proof that Spark always uses sort aggregation.

Always verify the actual physical plan.

---

# 9. Physical Plan Inspection

Run:

```scala
result.explain("formatted")
```

Look for:

```text
SortAggregate
```

or:

```text
SortAggregateExec
```

Also inspect whether a:

```text
Sort
```

operator appears before the aggregation.

A conceptual plan may look like:

```text
SortAggregate
+- Sort
   +- Exchange
      +- SortAggregate
         +- Scan
```

This indicates that sorting is part of the aggregation strategy.

---

# 10. Understanding the Execution Flow

A simplified execution flow is:

```text
                    Input Data
                        |
                        v
                  Partial Processing
                        |
                        v
                     Exchange
                        |
                        v
                Partitioned Data
                        |
                        v
                   Sort by Key
                        |
                        v
                SortAggregate
                        |
                        v
                  Final Result
```

The shuffle ensures that rows with the same:

```text
customer_id
```

are brought into the same partition.

The sort then organizes those rows:

```text
101
101
101
102
102
103
103
103
```

The aggregation can process each group sequentially.

---

# 11. Why Sorting Helps Aggregation

Consider this input:

```text
101
102
101
103
102
101
```

A sequential aggregation cannot immediately finish customer `101` because another `101` may appear later.

With sorting:

```text
101
101
101
102
102
103
```

Spark knows that once it moves from:

```text
101
```

to:

```text
102
```

the `101` group is complete.

Conceptually:

```text
101
101
101
---
group complete

102
102
---
group complete

103
---
group complete
```

This property makes sort-based aggregation suitable for streaming through ordered records.

---

# 12. Memory Behavior

This is one of the most important concepts in this experiment.

## Hash Aggregation

Hash aggregation needs structures similar to:

```text
HashMap[
    customer_id -> aggregation state
]
```

If the number of groups becomes very large:

```text
1,000,000 customers
10,000,000 customers
100,000,000 customers
```

the aggregation state can become significant.

---

## Sort Aggregation

Sort-based aggregation does not fundamentally require maintaining the same large hash map of all groups.

Instead, after sorting:

```text
customer 101
customer 101
customer 101
```

can be processed as one contiguous group.

Then:

```text
customer 102
customer 102
```

can be processed.

The conceptual memory pattern becomes closer to:

```text
Current Group
     |
     v
101 -> running total
     |
     v
finish 101

Current Group
     |
     v
102 -> running total
```

However:

> Sort-based aggregation is not automatically memory-free.

Sorting itself can require significant memory, spilling, disk I/O, and CPU.

Therefore the correct comparison is:

```text
Hash aggregation
    memory pressure from aggregation state

Sort aggregation
    CPU + memory + possible spill pressure from sorting
```

---

# 13. CPU Cost

Sorting introduces additional computational work.

For `N` records, comparison-based sorting is generally associated with approximately:

```text
O(N log N)
```

complexity.

Hash lookup is approximately:

```text
O(1)
```

average-case lookup.

Therefore, when hash aggregation is practical:

```text
Hash aggregation
       ↓
usually cheaper
```

while:

```text
Sort aggregation
       ↓
sorting overhead
```

can make it slower.

This is why sort-based aggregation should not be assumed to be faster merely because it may reduce hash-map state.

---

# 14. Shuffle Interaction

Aggregation with:

```text
GROUP BY customer_id
```

normally requires all rows for a particular customer to reach the same partition.

Conceptually:

```text
Partition 0 ----\
Partition 1 -----\
Partition 2 ------> Shuffle by customer_id
Partition 3 -----/
Partition 4 ----/
```

After the shuffle:

```text
Partition A
101
101
101
105
105

Partition B
102
102
109
109
```

Each partition can then perform local aggregation.

With sort-based aggregation, the data may additionally be sorted by the grouping key:

```text
Partition A
101
101
101
105
105
```

---

# 15. Spark UI Investigation

Run the benchmark and open:

```text
http://localhost:4040
```

Inspect:

## SQL Tab

Find the aggregation query.

Record:

```text
Execution time
```

and inspect the physical execution graph.

---

## DAG Visualization

Look for:

```text
Scan
  |
  v
Exchange
  |
  v
Sort
  |
  v
SortAggregate
```

The exact structure can differ depending on the query.

---

# 16. Important SQL Metrics

Pay attention to:

### Input Size

How much data entered the aggregation?

```text
Input Size
```

---

### Shuffle Read

How much data was transferred into the aggregation stage?

```text
Shuffle Read
```

---

### Shuffle Write

How much data was written during shuffle?

```text
Shuffle Write
```

---

### Sort Time

If available in the SQL metrics, inspect:

```text
Sort Time
```

This helps identify the overhead introduced by sorting.

---

### Spill Metrics

Watch for:

```text
Memory Bytes Spilled
Disk Bytes Spilled
```

Large spill values indicate memory pressure.

---

# 17. Experiment Matrix

Run the same aggregation workload under different conditions.

| Experiment | Rows | Customers | Shuffle Partitions | Expected Focus         |
| ---------- | ---: | --------: | -----------------: | ---------------------- |
| A          |  10M |        1M |                 64 | Baseline               |
| B          |  10M |        1M |                 32 | Fewer partitions       |
| C          |  10M |        1M |                128 | More partitions        |
| D          |  10M |      100K |                 64 | Fewer groups           |
| E          |  10M |        5M |                 64 | Many groups            |
| F          |  10M |        1M |                 64 | Sort-based aggregation |

The goal is not simply to find the fastest configuration.

The goal is to understand:

```text
Data volume
    +
Number of groups
    +
Partition count
    +
Aggregation strategy
    |
    v
Execution behavior
```

---

# 18. Expected Observations

The sort-based experiment may show:

```text
Sort
  |
  v
additional CPU cost
```

and potentially:

```text
Memory Bytes Spilled
Disk Bytes Spilled
```

if sorting exceeds available memory.

Compared with hash aggregation, you may observe:

```text
HashAggregate
    ↓
lower sort overhead

SortAggregate
    ↓
higher sorting overhead
```

But the exact result depends heavily on:

* Number of groups
* Data distribution
* Partition count
* Memory available
* Data types
* Aggregation function
* AQE
* Spark version
* Input data characteristics

---

# 19. Why the Number of Groups Matters

Consider:

```text
10M rows
100K customers
```

versus:

```text
10M rows
5M customers
```

The second workload creates many more aggregation groups.

For hash aggregation:

```text
5M groups
    |
    v
large aggregation state
```

For sort aggregation:

```text
5M groups
    |
    v
large sorting workload
```

Therefore the bottleneck changes.

This is a key Spark engineering lesson:

> Aggregation performance depends not only on row count but also on cardinality of the grouping key.

---

# 20. Sort-Based Aggregation and Skew

Consider:

```text
customer_id = 999
```

appearing:

```text
8,000,000 times
```

while most other customers appear only a few times.

This creates a skewed key:

```text
999 -> 8M rows
other customers -> small number of rows
```

The shuffle can become imbalanced.

Conceptually:

```text
Partition 0 -> 100 MB
Partition 1 -> 120 MB
Partition 2 -> 95 MB
Partition 3 -> 8 GB  <-- skew
```

Sort-based aggregation does not eliminate shuffle skew.

Therefore:

> Sorting is an aggregation strategy, not a general solution to data skew.

---

# 21. AQE Interaction

Adaptive Query Execution can dynamically optimize execution.

Important AQE features include:

```text
Coalescing shuffle partitions
```

and:

```text
Skew join optimization
```

For aggregation workloads, AQE may change the effective partitioning behavior after shuffle statistics become available.

Therefore, when comparing experiments, record:

```text
Configured shuffle partitions
```

and:

```text
Actual execution behavior
```

Do not assume:

```text
spark.sql.shuffle.partitions = 64
```

means exactly 64 effective partitions are processed throughout the entire query.

---

# 22. Physical Plan vs Configuration

A very important engineering principle:

```text
Configuration
     ≠
Actual execution strategy
```

For example:

```text
spark.sql.shuffle.partitions = 64
```

does not tell us:

```text
whether HashAggregateExec or SortAggregateExec was used
```

The physical plan does.

Therefore always validate with:

```scala
result.explain("formatted")
```

and Spark UI metrics.

---

# 23. Benchmark Recording Template

Record the experiment using the following format:

```text
Experiment: Module 1.7.5
Rows: 10,000,000
Customers: 1,000,000
Shuffle Partitions: 64

Aggregation:
SUM(amount) GROUP BY customer_id

Physical Operator:
SortAggregateExec

Sort Present:
Yes / No

AQE:
Enabled / Disabled

Execution Time:
_____ seconds

Shuffle Read:
_____ MB

Shuffle Write:
_____ MB

Memory Spill:
_____ MB

Disk Spill:
_____ MB

Notes:
____________________________
```

---

# 24. Comparison With Previous Modules

Build a comparison table:

| Strategy             | Main Idea                       | Strength                      | Weakness                        |
| -------------------- | ------------------------------- | ----------------------------- | ------------------------------- |
| Baseline aggregation | Spark chooses strategy          | Realistic                     | Less controlled                 |
| Hash aggregation     | Hash map by key                 | Fast lookups                  | Aggregation state memory        |
| Partial aggregation  | Aggregate before shuffle        | Reduces shuffle data          | Requires compatible aggregation |
| Ordered aggregation  | Sorting/order affects execution | Useful for ordered processing | Sorting cost                    |
| Sort aggregation     | Sort keys then aggregate        | Sequential grouped processing | Sort CPU/memory/spill           |

---

# 25. Key Engineering Insight

A common misconception is:

```text
SortAggregate = better memory usage = faster
```

This is incorrect.

The correct mental model is:

```text
HashAggregate
    |
    +--> efficient lookup
    +--> usually preferred
    +--> aggregation-state memory

SortAggregate
    |
    +--> ordered input
    +--> sequential grouping
    +--> sorting overhead
    +--> potential spill
```

The best strategy depends on workload characteristics.

---

# 26. Interview Questions

## Q1. What is SortAggregateExec?

`SortAggregateExec` is Spark's physical execution operator for performing aggregation when the execution strategy relies on sorted input.

---

## Q2. How is SortAggregate different from HashAggregate?

Hash aggregation maintains aggregation state using hash-based structures.

Sort aggregation processes records ordered by grouping keys and aggregates consecutive records belonging to the same group.

---

## Q3. Why doesn't Spark always use SortAggregate?

Because sorting can be expensive.

When hash aggregation is efficient and practical, Spark can generally achieve aggregation without paying the additional sorting cost.

---

## Q4. Does SortAggregate eliminate shuffle?

No.

If the grouping key is not already partitioned appropriately, Spark still generally needs a shuffle so that rows belonging to the same key reach the same partition.

---

## Q5. Does SortAggregate eliminate memory pressure?

No.

It changes the memory/execution characteristics but sorting can itself consume memory and may spill to disk.

---

## Q6. What metrics would you inspect?

At minimum:

```text
Execution Time
Shuffle Read
Shuffle Write
Sort Time
Memory Spill
Disk Spill
```

---

## Q7. How would you determine whether Spark actually used sort aggregation?

Inspect:

```scala
result.explain("formatted")
```

and look for:

```text
SortAggregate
```

and related:

```text
Sort
```

operators.

---

## Q8. Can AQE change the execution behavior?

Yes.

AQE can dynamically modify execution characteristics after runtime statistics become available, so the Spark UI should be inspected in addition to the initial logical/physical plan.

---

# 27. Production Engineering Perspective

When diagnosing a slow aggregation in production, do not immediately change:

```text
spark.sql.shuffle.partitions
```

Instead investigate systematically:

```text
1. Input data volume
2. Number of grouping keys
3. Key cardinality
4. Data skew
5. Partial aggregation
6. Shuffle volume
7. Physical aggregation operator
8. Sort cost
9. Spill
10. AQE behavior
11. Partition sizing
12. Executor memory
```

The goal is to identify the actual bottleneck.

---

# 28. Final Mental Model

Remember the following:

```text
GROUP BY
   |
   +---------------------------+
   |                           |
   v                           v
Hash Aggregation          Sort Aggregation
   |                           |
Hash map                    Sort by key
   |                           |
Running state              Consecutive groups
   |                           |
   +-------------+-------------+
                 |
                 v
             Aggregated
               Result
```

The most important principle is:

> **Spark aggregation performance is determined by the interaction between aggregation strategy, grouping-key cardinality, partitioning, shuffle volume, sorting cost, memory pressure, skew, and AQE—not by the aggregation operator alone.**

---

# 29. Experiment Completion Checklist

* [ ] Run the aggregation benchmark.
* [ ] Confirm the physical aggregation operator.
* [ ] Confirm whether `SortAggregate` is present.
* [ ] Check for `Sort` operators.
* [ ] Record execution time.
* [ ] Record shuffle read/write.
* [ ] Record sort metrics if available.
* [ ] Check memory spill.
* [ ] Check disk spill.
* [ ] Inspect the Spark SQL DAG.
* [ ] Compare against hash aggregation.
* [ ] Compare different customer cardinalities.
* [ ] Compare different shuffle partition counts.
* [ ] Observe AQE behavior.
* [ ] Document the final physical plan.
* [ ] Add conclusions to the benchmark notes.

---

# 30. Final Takeaway

Sort-based aggregation is an important Spark execution strategy because it demonstrates a fundamental distributed-computing trade-off:

```text
Hashing
    vs
Sorting
```

Hash aggregation typically provides efficient key lookup but requires maintaining aggregation state.

Sort aggregation introduces sorting work but allows aggregation over ordered keys using sequential processing.

For a Lead Big Data Engineer, the important skill is not memorizing:

```text
SortAggregateExec
```

but being able to answer:

```text
Why did Spark choose this strategy?
What is the cost?
Where is the bottleneck?
What does the Spark UI show?
How does cardinality affect it?
How does partitioning affect it?
Is the query spilling?
Is AQE changing the execution?
What would I change in production?
```

That is the real objective of this experiment.
