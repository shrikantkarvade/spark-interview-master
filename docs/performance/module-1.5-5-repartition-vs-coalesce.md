# Module 1.5.5 — Repartition vs Coalesce: Shuffle Cost and Performance

## 1. Engineering Question

When a Spark application changes the number of partitions, what is the actual execution cost?

More specifically:

* Does changing partition count require a shuffle?
* Why does `repartition()` introduce an `Exchange`?
* Why does `coalesce()` avoid a shuffle when reducing partitions?
* How does the change affect Spark stages and tasks?
* What performance difference can be observed?

This experiment compares:

```text
repartition(8)
```

with:

```text
coalesce(1)
```

starting from the same 1,000,000-row DataFrame.

---

# 2. Use Case

Consider a production Spark pipeline processing a large dataset.

During the pipeline, an engineer may decide that the data has too many or too few partitions.

For example:

```scala
df.repartition(200)
```

might be used to increase parallelism or redistribute data.

Conversely:

```scala
df.coalesce(20)
```

might be used before writing a relatively small result to avoid creating hundreds of small output files.

These operations look deceptively similar because both change the partition count.

Internally, however, they are very different.

The key production question is therefore:

> **Is the application merely reducing the number of existing partitions, or does it need to redistribute data across a new partition layout?**

---

# 3. Problem

A common misconception is:

> "`repartition()` and `coalesce()` both just change the number of partitions."

That description hides the most important performance characteristic.

Changing the partition count can involve:

```text
Data redistribution
        ↓
Shuffle
        ↓
Exchange
        ↓
Additional stage boundary
        ↓
Additional task execution
        ↓
Potential network I/O
        ↓
Higher execution cost
```

If the application only needs to reduce the number of partitions, performing a full shuffle may be unnecessary.

This experiment demonstrates that distinction using Spark's actual execution plan and runtime stages.

---

# 4. Experimental Setup

Environment:

| Property           | Value      |
| ------------------ | ---------- |
| Spark              | 3.5.1      |
| Java               | 17.0.20.1  |
| Scala              | 2.12.18    |
| Execution mode     | `local[2]` |
| Input rows         | 1,000,000  |
| Initial partitions | 2          |
| Repartition target | 8          |
| Coalesce target    | 1          |
| Action             | `count()`  |
| AQE                | Enabled    |
| OS                 | Windows 11 |

The input DataFrame is:

```scala
val df =
  spark.range(1000000)
```

The baseline contains two partitions because the application runs with:

```text
local[2]
```

and the Range workload uses two splits in this experiment.

---

# 5. Experiment

The benchmark executes two independent partition transformations against the same logical input.

## Repartition

```scala
val repartitioned =
  df.repartition(8)

val repartitionStart =
  System.nanoTime()

val repartitionCount =
  repartitioned.count()

val repartitionTime =
  (System.nanoTime() - repartitionStart) / 1000000
```

## Coalesce

```scala
val coalesced =
  df.coalesce(1)

val coalesceStart =
  System.nanoTime()

val coalesceCount =
  coalesced.count()

val coalesceTime =
  (System.nanoTime() - coalesceStart) / 1000000
```

Both operations process the same 1,000,000 rows.

---

# 6. Baseline

The experiment reported:

```text
Baseline partitions = 2
```

Therefore the transformations are:

```text
repartition:

2 → 8


coalesce:

2 → 1
```

This is important because `repartition()` is increasing the number of partitions while `coalesce()` is reducing them.

---

# 7. Repartition(8) — Runtime Evidence

The benchmark reported:

```text
Partitions = 8
Rows = 1000000
Execution time = 1644 ms
```

The Spark scheduler logs provide strong evidence that a shuffle occurred.

The application registered the RDD as input to a shuffle:

```text
Registering RDD ... as input to shuffle
```

Spark then created:

```text
ShuffleMapStage 0
```

with:

```text
2 output partitions
```

The stage submitted:

```text
2 tasks
```

This corresponds to the two original input partitions.

Spark then processed the repartitioned data using an 8-way downstream partition layout.

The scheduler subsequently reported:

```text
Got map stage job ... with 8 output partitions
```

and submitted:

```text
8 missing tasks
```

The benchmark therefore demonstrates the two sides of the shuffle:

```text
Original data
     │
     │ 2 input partitions
     ▼
ShuffleMapStage
     │
     │ shuffle
     ▼
8 output partitions
     │
     ▼
8 downstream tasks
```

---

# 8. Repartition Physical Plan

Spark produced:

```text
== Physical Plan ==
AdaptiveSparkPlan (3)
+- Exchange (2)
   +- Range (1)


(1) Range
Output [1]: [id#0L]
Arguments: Range (0, 1000000, step=1, splits=Some(2))

(2) Exchange
Input [1]: [id#0L]
Arguments: RoundRobinPartitioning(8), REPARTITION_BY_NUM, [plan_id=100]

(3) AdaptiveSparkPlan
Output [1]: [id#0L]
Arguments: isFinalPlan=false
```

The most important operator is:

```text
Exchange
```

with:

```text
RoundRobinPartitioning(8)
```

Because no partitioning key was supplied, Spark uses round-robin distribution.

Conceptually:

```text
Range
2 partitions
     │
     ▼
Exchange
RoundRobinPartitioning(8)
     │
     ▼
8 partitions
```

This is the physical-plan representation of the redistribution.

---

# 9. Repartition Does Not Simply "Create 8 Tasks"

A subtle but important observation is the distinction between **upstream tasks** and **downstream tasks**.

The original Range has:

```text
2 partitions
```

Therefore the upstream shuffle stage processes:

```text
2 tasks
```

The shuffle creates an 8-way partitioned result.

The downstream stage then consumes:

```text
8 partitions
```

and therefore has:

```text
8 tasks
```

The execution is therefore better represented as:

```text
              2 tasks
                │
                ▼
        ┌─────────────────┐
        │ Shuffle /       │
        │ Exchange        │
        │ RoundRobin(8)   │
        └─────────────────┘
                │
       ┌────────┼────────┐
       ▼        ▼        ▼
      P0       P1       ... P7
       │        │          │
       ▼        ▼          ▼
     Task     Task       Task
```

This is much more accurate than saying:

> "`repartition(8)` runs eight shuffle tasks."

The upstream work is performed according to the existing partition layout; the resulting shuffle output is consumed according to the new partition layout.

---

# 10. Shuffle Fetch Evidence

The runtime also reported:

```text
Getting 2 (2.5 KiB) non-empty blocks
including 2 (2.5 KiB) local
and 0 remote blocks
```

This experiment was executed on a single Windows machine using:

```text
local[2]
```

Therefore Spark fetched the shuffle blocks locally.

This should **not** be interpreted as meaning that a production cluster would have no network cost.

On a distributed cluster, shuffle data can be written by one executor and fetched by another executor across the network.

Therefore:

```text
Local benchmark:

shuffle → local block transfer


Production cluster:

shuffle → potentially network transfer
```

This distinction is critical when extrapolating local benchmark results to production.

---

# 11. Coalesce(1) — Runtime Evidence

The benchmark reported:

```text
Partitions = 1
Rows = 1000000
Execution time = 145 ms
```

The scheduler reported:

```text
Got job ... with 1 output partitions
```

and:

```text
Submitting 1 missing tasks
```

Most importantly, there was no preceding ShuffleMapStage.

The result was executed as a single ResultStage.

This provides direct runtime evidence that the Dataset/DataFrame `coalesce(1)` operation reduced the partition count without introducing a shuffle in this experiment.

---

# 12. Coalesce Physical Plan

Spark produced:

```text
== Physical Plan ==
Coalesce (2)
+- * Range (1)


(1) Range [codegen id : 1]
Output [1]: [id#0L]
Arguments: Range (0, 1000000, step=1, splits=Some(2))

(2) Coalesce
Input [1]: [id#0L]
Arguments: 1
```

Notice what is missing:

```text
Exchange
```

There is no shuffle boundary in the physical plan.

Conceptually:

```text
Range
2 partitions
     │
     ▼
Coalesce
     │
     ▼
1 partition
```

This is a narrow dependency in this DataFrame operation.

---

# 13. Performance Results

The measured results were:

| Metric            | Repartition(8) | Coalesce(1) |
| ----------------- | -------------: | ----------: |
| Input partitions  |              2 |           2 |
| Output partitions |              8 |           1 |
| Rows              |      1,000,000 |   1,000,000 |
| Exchange          |            Yes |          No |
| Shuffle           |            Yes |          No |
| Downstream tasks  |              8 |           1 |
| Execution time    |       1,644 ms |      145 ms |

The measured ratio was approximately:

```text
1644 / 145 ≈ 11.3
```

So `repartition(8)` took approximately **11.3× longer** than `coalesce(1)` in this particular local benchmark.

---

# 14. What the 11.3× Result Does — and Does Not — Mean

The result is useful evidence, but it should not be generalized into:

> "`repartition()` is always 11× slower than `coalesce()`."

That would be an invalid production conclusion.

The absolute timings depend on:

* dataset size
* data type
* cluster size
* executor resources
* network bandwidth
* serialization
* JVM state
* code-generation warm-up
* number of input partitions
* target partition count
* shuffle volume
* storage characteristics
* AQE configuration

This benchmark uses:

```text
1,000,000 rows
local[2]
single machine
```

Therefore the most defensible conclusion is:

> **In this controlled local workload, the shuffle introduced by `repartition(8)` produced substantially higher execution cost than the narrow `coalesce(1)` operation.**

The architectural difference is more important than the exact timing.

---

# 15. Solution

The correct operation depends on the engineering objective.

## Use repartition when data redistribution is required

For example:

```scala
df.repartition(200)
```

or:

```scala
df.repartition(200, $"customer_id")
```

Use this when the application needs to establish a new partition distribution.

Typical reasons include:

* increasing parallelism
* distributing data across more partitions
* partitioning by a key
* preparing data for downstream operations
* reducing partition imbalance
* establishing a useful partitioning scheme before expensive processing

The trade-off is the shuffle.

---

## Use coalesce when reducing partitions is sufficient

For example:

```scala
df.coalesce(20)
```

This is useful when the current partition layout is already acceptable and the objective is simply to reduce the number of partitions.

Typical examples include:

* reducing small output files
* reducing the number of downstream tasks
* shrinking a relatively small result set
* reducing unnecessary task overhead

The key benefit is avoiding a full redistribution shuffle.

---

# 16. Production Decision Rule

A practical decision framework is:

```text
Need to change partition count?
            │
            ▼
     Is redistribution
       required?
        /       \
      Yes       No
       │         │
       ▼         ▼
repartition   coalesce
       │         │
       ▼         ▼
   Shuffle     Narrow
 dependency   dependency
```

More specifically:

### Increasing partitions

If you need:

```text
2 → 8
```

then `coalesce(8)` will not give you eight partitions when the input only has two.

In the earlier experiment:

```text
coalesce(8)
```

left the DataFrame at:

```text
2 partitions
```

Therefore:

> **Coalesce is not a mechanism for increasing parallelism.**

If you genuinely need eight partitions, `repartition(8)` is the appropriate operation.

---

### Reducing partitions

If you need:

```text
8 → 1
```

and redistribution is unnecessary:

```scala
df.coalesce(1)
```

can avoid the shuffle associated with:

```scala
df.repartition(1)
```

However, that does not mean `coalesce(1)` is always a good production choice.

A single partition means:

```text
1 task
```

and therefore can become a bottleneck for a large dataset.

The correct question is not:

> "Can I reduce the data to one partition?"

It is:

> **"What partition count gives the downstream operation appropriate parallelism and output characteristics?"**

---

# 17. Important Production Caveat: `coalesce(1)`

The benchmark shows that:

```scala
coalesce(1)
```

was much faster than:

```scala
repartition(8)
```

in this small local workload.

That does **not** mean:

> "Always use coalesce(1) for performance."

For a large production dataset, forcing everything into one partition can create:

* a single-task bottleneck
* poor CPU utilization
* long-running tasks
* increased executor memory pressure
* poor scalability
* potentially large output files

For example:

```text
100 GB
   ↓
coalesce(1)
   ↓
one task processes 100 GB
```

is generally a very different performance profile from:

```text
100 GB
   ↓
200 partitions
   ↓
many parallel tasks
```

Therefore partition reduction must always be considered together with data volume and downstream workload.

---

# 18. Repartition vs Coalesce — Internal View

The core difference can be summarized as:

```text
REPARTITION

Existing partitions
       │
       ▼
     Shuffle
       │
       ▼
New distribution
       │
       ▼
New partition count
```

versus:

```text
COALESCE

Existing partitions
       │
       ▼
Narrow dependency
       │
       ▼
Fewer partitions
```

The difference is not merely an API preference.

It changes the execution graph.

---

# 19. Key Spark Internals Lesson

A partition-count API call can introduce a physical execution boundary.

The source code:

```scala
df.repartition(8)
```

looks like a simple transformation.

The physical plan reveals something much more significant:

```text
Exchange
RoundRobinPartitioning(8)
```

That `Exchange` represents a redistribution boundary.

This is exactly why reading the physical plan is essential when diagnosing Spark performance.

Instead of asking:

> "Which API is faster?"

a production engineer should ask:

> **"What physical dependency did this API introduce?"**

---

# 20. Interview-Level Takeaways

### Q: What is the main difference between repartition and coalesce?

`repartition()` performs a shuffle to redistribute data into the requested partition layout, while DataFrame/Dataset `coalesce()` reduces partitions without introducing a shuffle in this execution model.

### Q: Can coalesce increase partitions?

No. If the requested number is greater than the existing partition count, it does not increase parallelism.

### Q: Why is repartition more expensive?

Because it introduces an `Exchange` and shuffle boundary, requiring data redistribution.

### Q: Does repartition always involve network traffic?

Not necessarily in local execution. In a distributed cluster, shuffle data may cross executor/network boundaries.

### Q: Does coalesce always mean one task?

No. `coalesce(n)` targets `n` partitions. `coalesce(1)` produces one partition, which results in one downstream task for the relevant action.

### Q: Should we always use coalesce to avoid shuffle?

No. If data must be redistributed for parallelism, key distribution, joins, or downstream processing, avoiding the shuffle may produce a worse overall execution plan.

---

# 21. Evidence Summary

This experiment established the following with actual Spark 3.5.1 execution evidence:

```text
Initial Range
2 partitions
```

### `repartition(8)`

```text
2 partitions
      ↓
Exchange
      ↓
RoundRobinPartitioning(8)
      ↓
8 partitions
```

Observed:

```text
ShuffleMapStage
2 upstream tasks
8 downstream tasks
Exchange
Shuffle
1,644 ms
```

### `coalesce(1)`

```text
2 partitions
      ↓
Coalesce
      ↓
1 partition
```

Observed:

```text
No ShuffleMapStage
No Exchange
1 downstream task
145 ms
```

---

# 22. Final Engineering Lesson

Partition count is not merely metadata.

Changing partitioning can change:

* the dependency graph
* stage boundaries
* shuffle behavior
* task parallelism
* network traffic
* scheduler overhead
* memory pressure
* downstream performance

The critical distinction is:

> **`repartition()` changes data distribution; `coalesce()` reduces the existing partition layout without a shuffle in the DataFrame/Dataset execution demonstrated here.**

Therefore, a production Spark engineer should never choose between them purely based on the desired partition count.

The decision should be based on:

```text
Data volume
+
Required parallelism
+
Data distribution
+
Downstream operations
+
Shuffle cost
+
Output characteristics
```

That is the difference between simply manipulating partition counts and actually designing Spark execution.
