# Module 1.4.10 — AQE Shuffle Partition Coalescing

## Objective

Understand how **Adaptive Query Execution (AQE)** can modify Spark's execution plan at runtime based on statistics collected after shuffle materialization.

The specific question for this experiment is:

> If Spark initially plans a shuffle with 20 partitions, can AQE recognize that the actual shuffle output is very small and coalesce the downstream shuffle reads?

This experiment focuses specifically on **AQE shuffle partition coalescing**.

It does not attempt to demonstrate AQE join conversion.

---

## Environment

| Property                   | Value      |
| -------------------------- | ---------- |
| Apache Spark               | 3.5.1      |
| Java                       | 17.0.20.1  |
| Scala                      | 2.12.18    |
| Gradle                     | 9.6.0      |
| OS                         | Windows 11 |
| Execution mode             | `local[2]` |
| Initial shuffle partitions | 20         |
| AQE                        | Enabled    |
| AQE partition coalescing   | Enabled    |
| Advisory partition size    | 1 MiB      |

`local[2]` comes from the project's development Spark configuration. The experiment is intended to demonstrate Spark planning behavior rather than represent a production cluster topology.

---

# Configuration

The experiment explicitly configured the following runtime SQL properties:

```scala
spark.conf.set("spark.sql.shuffle.partitions", 20)

spark.conf.set(
  "spark.sql.adaptive.enabled",
  true
)

spark.conf.set(
  "spark.sql.adaptive.coalescePartitions.enabled",
  true
)

spark.conf.set(
  "spark.sql.adaptive.advisoryPartitionSizeInBytes",
  1 * 1024 * 1024
)
```

The important point is that Spark starts with:

```text
20 shuffle partitions
```

while AQE is allowed to reconsider how those shuffle partitions are consumed after runtime statistics become available.

---

# Query

The experiment groups one million input rows into only 100 distinct keys:

```scala
val result =
  spark.range(1_000_000)
    .select(
      ($"id" % 100).as("key")
    )
    .groupBy($"key")
    .count()
```

The query therefore has:

```text
1,000,000 input rows
        ↓
id % 100
        ↓
100 distinct grouping keys
        ↓
groupBy(key)
        ↓
count()
```

This creates a useful AQE scenario:

* the input is relatively large,
* the grouping cardinality is very small,
* the shuffle is therefore expected to contain very little actual data.

---

# 1. Optimized Logical Plan

Spark's optimized logical plan was:

```text
Aggregate [key#16L], [key#16L, count(1) AS count#20L]
+- Project [(id#14L % 100) AS key#16L]
   +- Range (0, 1000000, step=1, splits=Some(2))
```

The logical plan contains:

1. a `Range` producing one million rows,
2. a `Project` deriving the grouping key,
3. an `Aggregate` performing the `groupBy` and `count`.

At this stage Spark is still describing **what the query means**, rather than exactly how it will execute.

---

# 2. Initial Physical Plan

The initial physical plan was:

```text
AdaptiveSparkPlan isFinalPlan=false
+- HashAggregate(
      keys=[key#16L],
      functions=[count(1)],
      output=[key#16L, count#20L]
   )
   +- Exchange
         hashpartitioning(key#16L, 20),
         ENSURE_REQUIREMENTS
      +- HashAggregate(
            keys=[key#16L],
            functions=[partial_count(1)],
            output=[key#16L, count#24L]
         )
         +- Project [(id#14L % 100) AS key#16L]
            +- Range (
                 0,
                 1000000,
                 step=1,
                 splits=2
               )
```

The critical part is:

```text
Exchange hashpartitioning(key, 20)
```

Spark initially planned a shuffle with **20 partitions**.

The physical execution shape is therefore:

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

The `Exchange` represents the shuffle boundary required to bring identical grouping keys together.

---

# 3. Why Does Spark Need the Exchange?

The input data is initially distributed according to the `Range` operation.

Spark cannot assume that all rows with:

```text
key = 42
```

are located in the same partition.

Therefore, before the final aggregation, Spark needs to repartition the data according to:

```text
hash(key)
```

This is represented by:

```text
Exchange hashpartitioning(key, 20)
```

The first aggregation is a **partial aggregation**.

Instead of shuffling every input row directly, Spark can first combine rows locally:

```text
1,000,000 input rows
        ↓
partial aggregation
        ↓
much smaller shuffle data
        ↓
Exchange
        ↓
final aggregation
```

This is one of the important reasons why aggregation queries do not necessarily shuffle the same volume of data as their input size suggests.

---

# 4. Runtime Statistics

The most important evidence from execution was:

```text
ShuffleMapStage 0
Tasks: 2
Duration: ~0.593 s
```

Spark also reported:

```text
advisory target size: 1048576
actual target size: 1048576
minimum partition size: 1048576
```

The advisory target corresponds to:

```text
1 MiB
```

More importantly, the materialized shuffle query stage contained only:

```text
Statistics:
sizeInBytes = 4.7 KiB
rowCount = 200
```

This is dramatically smaller than the original input:

```text
Input rows:       1,000,000
Shuffle rows:           200
```

The reason is the partial aggregation.

Each of the two input partitions can locally reduce the million input rows down to a small number of key/count combinations before the shuffle.

---

# 5. AQE Gets Runtime Information

Before execution, Spark had only the static plan.

It knew:

```text
spark.sql.shuffle.partitions = 20
```

but it did not yet know the actual runtime size of the shuffle output.

After the shuffle stage materialized, Spark had concrete runtime statistics:

```text
Shuffle size: approximately 4.7 KiB
Rows: 200
```

AQE can now use those statistics to reconsider downstream execution.

This is the key architectural difference:

```text
Static planning
      ↓
Initial physical plan
      ↓
Execute shuffle stage
      ↓
Collect runtime statistics
      ↓
AQE re-optimization
      ↓
Final physical plan
```

---

# 6. Final Executed Plan

After:

```scala
result.collect()
```

the formatted physical plan showed:

```text
== Physical Plan ==
AdaptiveSparkPlan
+- == Final Plan ==
   * HashAggregate
   +- AQEShuffleRead
      +- ShuffleQueryStage
         Statistics(sizeInBytes=4.7 KiB, rowCount=200)
         +- Exchange
            +- * HashAggregate
               +- * Project
                  +- * Range

+- == Initial Plan ==
   HashAggregate
   +- Exchange
   +- HashAggregate
      +- Project
         +- Range
```

The most important line is:

```text
AQEShuffleRead
Arguments: coalesced
```

And the adaptive root reported:

```text
Arguments: isFinalPlan=true
```

This is direct evidence that AQE modified the execution plan after obtaining runtime statistics.

---

# 7. What Exactly Changed?

The initial plan contained:

```text
Exchange
```

followed by the normal downstream aggregation.

The final plan contains:

```text
ShuffleQueryStage
        ↓
AQEShuffleRead
        ↓
Final HashAggregate
```

The important distinction is:

> AQE did not eliminate the shuffle.

The shuffle was still necessary because the aggregation requires grouping keys to be colocated.

Instead, AQE changed **how the already-materialized shuffle output is read downstream**.

Specifically:

```text
Initial:
Exchange → downstream processing

Final:
Exchange → ShuffleQueryStage
                    ↓
              AQEShuffleRead
              (coalesced)
                    ↓
              downstream processing
```

This is the essence of AQE shuffle partition coalescing.

---

# 8. Execution Evidence

The final execution stage showed:

```text
ResultStage 2
Tasks: 1
Duration: ~0.221 s
```

The stage fetched:

```text
2 non-empty blocks
~4.0 KiB
0 remote fetches
```

Because this was running under:

```text
local[2]
```

the shuffle blocks were local.

The final stage therefore had very little data to process.

---

# 9. Important Caveat — Do Not Overclaim the Partition Count

One tempting conclusion would be:

> "Spark changed the 20 partitions into exactly 1 partition."

That conclusion should **not** be made from this experiment alone.

The evidence proves:

```text
Initial shuffle partition configuration = 20
```

and:

```text
Final plan contains AQEShuffleRead
Arguments: coalesced
```

It also shows:

```text
ResultStage = 1 task
```

However, the formatted plan does not explicitly expose the exact number of coalesced downstream partitions in this output.

Therefore the technically correct statement is:

> AQE coalesced the shuffle partitions for downstream reading based on the small runtime shuffle size.

We should not claim an exact:

```text
20 → 1
```

partition conversion unless we capture additional runtime evidence that explicitly proves that number.

This distinction matters when documenting Spark internals professionally.

---

# 10. Initial vs Final Plan

| Aspect                        | Initial Plan        | Final AQE Plan       |
| ----------------------------- | ------------------- | -------------------- |
| AQE                           | Enabled             | Enabled              |
| Adaptive plan                 | `isFinalPlan=false` | `isFinalPlan=true`   |
| Shuffle                       | Required            | Still required       |
| Shuffle partitions configured | 20                  | Runtime read adapted |
| Shuffle stage                 | Exchange            | ShuffleQueryStage    |
| Shuffle read                  | Normal              | `AQEShuffleRead`     |
| Coalescing                    | Not yet applied     | `coalesced`          |
| Runtime statistics            | Not available       | 4.7 KiB / 200 rows   |
| Final aggregation             | HashAggregate       | HashAggregate        |

The key observation is that **AQE adapts around the shuffle rather than removing a logically necessary shuffle**.

---

# 11. What This Demonstrates About AQE

This experiment demonstrates an important Spark execution principle:

> Spark's physical plan is not necessarily the final execution plan when AQE is enabled.

The initial plan is based largely on static information:

```text
20 configured shuffle partitions
```

After execution begins, Spark discovers:

```text
actual shuffle size = 4.7 KiB
actual rows = 200
```

AQE can then optimize the downstream execution:

```text
AQEShuffleRead(coalesced)
```

The resulting execution plan is therefore:

```text
Initial Physical Plan
        ↓
Runtime Execution
        ↓
Runtime Statistics
        ↓
AQE Re-optimization
        ↓
Final Physical Plan
```

---

# 12. Production Engineering Insight

A common production problem is choosing an appropriate value for:

```text
spark.sql.shuffle.partitions
```

A single static value is difficult to optimize across workloads.

For example:

```text
20 partitions
```

might be reasonable for one workload but excessive for another.

If the actual shuffle is only a few kilobytes, processing many tiny downstream partitions can create unnecessary task scheduling overhead.

Conversely, large production workloads may require substantially more parallelism.

AQE provides a mechanism for making this decision more dynamically.

Instead of relying entirely on:

```text
static configuration
```

Spark can use:

```text
runtime statistics
```

to adapt execution.

This is particularly valuable for workloads where data volume varies significantly between executions.

---

# 13. Why the Advisory Partition Size Matters

The experiment configured:

```text
spark.sql.adaptive.advisoryPartitionSizeInBytes
= 1 MiB
```

This gives AQE a target size when reasoning about shuffle partition coalescing.

The runtime shuffle was only:

```text
4.7 KiB
```

which is dramatically smaller than:

```text
1 MiB
```

Therefore, the shuffle output provides a strong opportunity for partition coalescing.

This illustrates an important principle:

> AQE decisions are based on actual runtime data characteristics, not merely the original input size or configured partition count.

---

# 14. AQE Does Not Mean "No Shuffle"

A common misconception is:

> "AQE removes shuffles."

That is incorrect.

In this experiment:

```text
Initial plan:
Exchange
```

and:

```text
Final plan:
Exchange → ShuffleQueryStage → AQEShuffleRead
```

The shuffle remains.

AQE instead optimizes what happens **after the shuffle statistics become available**.

A more accurate mental model is:

```text
AQE
├── can change join strategies
├── can coalesce shuffle partitions
├── can split skewed shuffle partitions
└── can apply other runtime-aware optimizations
```

It is not a mechanism for simply eliminating every shuffle.

---

# 15. Why Partial Aggregation Matters

This experiment also provides an important secondary lesson.

The query processes:

```text
1,000,000 input rows
```

but the materialized shuffle stage contains only:

```text
200 rows
```

because Spark performs:

```text
Partial HashAggregate
```

before the shuffle.

The execution shape is therefore:

```text
1,000,000 rows
       ↓
local partial aggregation
       ↓
200 rows
       ↓
shuffle
       ↓
final aggregation
```

This is a powerful Spark optimization.

It means that the network cost of an aggregation is often much smaller than the raw input size would suggest when the aggregation has strong local reduction.

---

# 16. How to Read This Plan Like a Production Engineer

When looking at this plan in Spark UI or `explain("formatted")`, the useful questions are:

### Question 1 — Where is the shuffle?

Look for:

```text
Exchange
```

Here:

```text
Exchange hashpartitioning(key, 20)
```

confirms the shuffle boundary.

### Question 2 — Is AQE active?

Look for:

```text
AdaptiveSparkPlan
```

Here:

```text
AdaptiveSparkPlan
```

is present.

### Question 3 — Did AQE actually finish adapting?

Look for:

```text
isFinalPlan=true
```

This experiment produced exactly that.

### Question 4 — Did AQE modify shuffle reading?

Look for:

```text
AQEShuffleRead
```

and:

```text
Arguments: coalesced
```

Both are present.

### Question 5 — What runtime statistics drove the decision?

Here:

```text
sizeInBytes = 4.7 KiB
rowCount = 200
```

These statistics explain why coalescing was beneficial.

---

# 17. Interview Takeaways

### Q: What is AQE?

Adaptive Query Execution allows Spark to modify parts of the physical execution plan using statistics collected during runtime.

### Q: What does shuffle partition coalescing do?

It combines small shuffle partitions for downstream processing so Spark does not unnecessarily process many tiny partitions.

### Q: Does AQE remove the shuffle?

Not necessarily.

In this experiment, the shuffle remained necessary, but AQE changed the downstream shuffle read to:

```text
AQEShuffleRead
Arguments: coalesced
```

### Q: Why can Spark make a better decision at runtime?

Because the optimizer can now see actual statistics such as:

```text
shuffle size
row count
partition sizes
```

rather than relying solely on static estimates.

### Q: How do you know AQE actually changed the plan?

The final plan contains:

```text
== Final Plan ==
```

with:

```text
AQEShuffleRead
Arguments: coalesced
```

and:

```text
AdaptiveSparkPlan
Arguments: isFinalPlan=true
```

### Q: What is the difference between the initial and final plan?

The initial plan is the physical plan Spark starts with.

The final plan reflects runtime adaptations made after Spark materializes query stages and obtains runtime statistics.

---

# 18. Evidence Summary

| Evidence                      | Observation                          | Interpretation                       |
| ----------------------------- | ------------------------------------ | ------------------------------------ |
| Initial shuffle configuration | 20 partitions                        | Static starting point                |
| Initial physical plan         | `Exchange hashpartitioning(key, 20)` | Shuffle required                     |
| AQE                           | Enabled                              | Runtime adaptation possible          |
| Coalescing                    | Enabled                              | Shuffle reads may be coalesced       |
| Advisory target               | 1 MiB                                | Target used by AQE                   |
| Shuffle statistics            | 4.7 KiB                              | Very small runtime shuffle           |
| Shuffle rows                  | 200                                  | Strong local aggregation reduction   |
| Final plan                    | `AQEShuffleRead`                     | AQE adapted shuffle reading          |
| AQE argument                  | `coalesced`                          | Partition coalescing occurred        |
| Final plan state              | `isFinalPlan=true`                   | Runtime plan finalized               |
| Final stage                   | 1 task                               | Small downstream workload            |
| Remote fetches                | 0                                    | All shuffle data local in this test  |
| Shuffle removal               | No                                   | Shuffle remained logically necessary |

---

# 19. Final Conclusion

This experiment demonstrates the difference between **static physical planning** and **adaptive runtime execution** in Spark.

Spark initially planned:

```text
HashAggregate
      ↓
Exchange
20 shuffle partitions
      ↓
HashAggregate
```

After executing the shuffle, Spark discovered:

```text
4.7 KiB
200 rows
```

The adaptive optimizer then produced a final execution plan containing:

```text
ShuffleQueryStage
      ↓
AQEShuffleRead
      Arguments: coalesced
      ↓
HashAggregate
```

with:

```text
isFinalPlan=true
```

The important production-level lesson is:

> **Do not assume that the physical plan printed before execution is necessarily the plan Spark ultimately uses. With AQE enabled, runtime statistics can cause Spark to adapt the execution plan after shuffle stages materialize.**

In this experiment, AQE did not eliminate the required shuffle. Instead, it recognized that the materialized shuffle output was extremely small and **coalesced the downstream shuffle reads**, reducing unnecessary parallelism for the tiny result.

That is the behavior a production Spark engineer should look for when diagnosing small-shuffle workloads, excessive task counts, and AQE effectiveness.
