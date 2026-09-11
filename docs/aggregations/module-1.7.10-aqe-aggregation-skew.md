# Module 1.7.10 — AQE + Aggregation Skew

## Status

**Completed**

This module investigates how Apache Spark behaves when an aggregation key is highly skewed, and what Adaptive Query Execution (AQE) and salting actually do in that situation.

The conclusions below are based on the executed Scala experiments, physical plans, console timings, and Spark UI evidence collected during the experiment run.

---

## 1. Objective

The objective of this experiment is to distinguish between:

1. Input-data skew
2. Aggregation-key skew
3. Shuffle-partition skew
4. Reducer/task skew
5. AQE post-shuffle partition coalescing
6. AQE skew handling versus generic aggregation skew
7. Manual salting as an aggregation optimization
8. The trade-off between optimization benefit and additional shuffle/aggregation stages

The most important question is:

> **If one aggregation key owns most of the input rows, will AQE automatically split that hot aggregation key across multiple reducers?**

The experiment demonstrates that the answer is **not generally yes**.

---

# 2. Core Learning

A common misconception is:

> "The data is skewed, so Spark will have a skewed shuffle partition, and AQE will automatically fix it."

That chain of reasoning is incomplete.

For an aggregation such as:

```text
groupBy(customer_id)
  .agg(count(*), sum(amount))
````

Spark normally performs a partial aggregation before the shuffle:

```text
Input
  ↓
Partial HashAggregate
  ↓
Exchange hashpartitioning(customer_id)
  ↓
Final HashAggregate
```

If millions of rows belong to the same customer, the partial aggregation can reduce those millions of rows into a small number of intermediate records **before the shuffle**.

Therefore:

```text
Highly skewed input
        ≠
Highly skewed shuffle data
        ≠
Highly skewed reducer workload
```

This distinction is one of the most important lessons from this module.

---

# 3. Experimental Dataset

The experiments used:

* Input rows: **5,000,000**
* Customers: approximately **1,000,000**
* Hot customer: `HOT_CUSTOMER`
* Hot-key rows: **4,000,000**
* Normal rows: **1,000,000**
* Hot-key percentage: **80%**
* Shuffle partitions: **64**

Therefore the input deliberately contained extreme aggregation-key skew:

```text
HOT_CUSTOMER     → 4,000,000 rows
Other customers  → 1,000,000 rows
```

The resulting aggregation produced:

```text
1,000,001 groups
```

---

# 4. Experiment Matrix

| Experiment | Dataset     | AQE |         Salting | Purpose                           |
| ---------- | ----------- | --: | --------------: | --------------------------------- |
| A          | Uniform     | OFF |              No | Baseline                          |
| B          | 80% hot key | OFF |              No | Observe skew without AQE          |
| C          | 80% hot key |  ON |              No | Observe what AQE actually changes |
| D          | 80% hot key |  ON | Yes, 16 buckets | Test manual aggregation salting   |

---

# 5. Experiment A — Uniform Data + AQE OFF

## Purpose

A provides the baseline against which the skewed experiments can be understood.

The data is distributed relatively uniformly across customers.

The core physical plan was:

```text
HashAggregate
  ↓
Exchange hashpartitioning(customer_id, 64)
  ↓
HashAggregate
  ↓
InMemoryTableScan
```

More specifically:

```text
*(2) HashAggregate(
    keys=[customer_id],
    functions=[count(1), sum(amount)]
)
+- Exchange hashpartitioning(customer_id, 64)
   +- *(1) HashAggregate(
       keys=[customer_id],
       functions=[partial_count(1), partial_sum(amount)]
   )
      +- InMemoryTableScan
```

## Spark UI evidence

The relevant stage showed:

* **64 tasks**
* Local shuffle reads
* **0 remote fetches**
* Typical non-empty shuffle blocks around **306–321 KiB**
* No obvious giant straggler
* Task durations were broadly similar

This represents the expected healthy baseline.

## Key lesson

A well-distributed aggregation generally produces relatively balanced reducer work.

---

# 6. Experiment B — 80% Hot Key + AQE OFF

## Configuration

```text
Rows                  = 5,000,000
Customers             = 1,000,000
Hot-key percentage    = 80%
Shuffle partitions    = 64
AQE                   = OFF
Salting               = OFF
```

## Result

```text
Aggregation result groups: 1,000,001
Aggregation execution time: 1.50 seconds
```

## Physical plan

```text
*(2) HashAggregate(
    keys=[customer_id],
    functions=[count(1), sum(amount)]
)
+- Exchange hashpartitioning(customer_id, 64)
   +- *(1) HashAggregate(
       keys=[customer_id],
       functions=[partial_count(1), partial_sum(amount)]
   )
      +- InMemoryTableScan
```

There is no:

```text
AdaptiveSparkPlan
```

and no:

```text
AQEShuffleRead
```

because AQE is disabled.

## Spark UI evidence

The aggregation execution showed:

* 64 shuffle partitions
* Approximately **2,000,000 shuffle records**
* No remote shuffle fetches
* No spill
* No sort fallback
* Downstream shuffle blocks were relatively small
* Pre-shuffle partial aggregation did significant reduction

In one supporting Spark UI action:

| Metric                            | Observation |
| --------------------------------- | ----------- |
| WholeStageCodegen maximum         | **1.4 s**   |
| Stage                             | **7.0**     |
| Task                              | **137**     |
| Pre-shuffle HashAggregate maximum | **1.1 s**   |
| Stage                             | **7.0**     |
| Task                              | **136**     |
| Shuffle records                   | ~2,000,000  |
| Shuffle bytes                     | ~18.6 MiB   |
| Remote bytes                      | 0           |
| Spill                             | 0           |

The UI action containing `TakeOrderedAndProject` was a later `show(10)` action, so these UI timings are supporting evidence rather than the authoritative benchmark timing.

## Important observation

Even with:

```text
80% of all input rows → HOT_CUSTOMER
```

the experiment did **not** demonstrate a corresponding catastrophic reducer skew.

Why?

Because Spark performs:

```text
Partial HashAggregate
```

before the shuffle.

The 4 million hot-key input rows can be combined locally into partial aggregation state.

Therefore Spark does not necessarily need to shuffle 4 million individual records for the hot customer.

---

# 7. Experiment C — 80% Hot Key + AQE ON

## Purpose

C asks:

> What does AQE actually do when the aggregation input is highly skewed?

## Configuration

```text
Rows                  = 5,000,000
Customers             = 1,000,000
Hot-key percentage    = 80%
Shuffle partitions    = 64
AQE                   = ON
Salting               = OFF
```

## Result

```text
Aggregation result groups: 1,000,001
Aggregation execution time: 0.84 seconds
```

This was faster than B in this run.

However, this single-node benchmark should not be interpreted as proof that AQE always provides exactly this improvement.

## Initial physical plan

```text
AdaptiveSparkPlan isFinalPlan=false
+- HashAggregate(
    keys=[customer_id],
    functions=[count(1), sum(amount)]
)
   +- Exchange hashpartitioning(customer_id, 64)
      +- HashAggregate(
          keys=[customer_id],
          functions=[partial_count(1), partial_sum(amount)]
      )
         +- InMemoryTableScan
```

The important difference is:

```text
AdaptiveSparkPlan
```

which indicates that the query is eligible for runtime adaptation.

---

# 8. What AQE Actually Did

The Spark runtime logs showed:

```text
ShufflePartitionsUtil
advisory target ≈ 3.19 MiB
minimum target = 1 MiB
```

The original plan had:

```text
64 shuffle partitions
```

AQE subsequently reduced the number of downstream partitions to approximately:

```text
64 → 2
```

The final runtime stages showed two downstream tasks.

This is evidence of:

> **AQE post-shuffle partition coalescing**

rather than generic aggregation-key skew splitting.

The strongest supporting Spark UI query was Query 11.

Its final physical plan contained:

```text
AdaptiveSparkPlan
+- == Final Plan ==
   TakeOrderedAndProject
   +- HashAggregate
      +- AQEShuffleRead(coalesced)
         +- ShuffleQueryStage
            +- Exchange hashpartitioning(customer_id, 64)
               +- HashAggregate(
                   partial_count(1),
                   partial_sum(amount)
               )
```

This proves that AQE produced:

```text
AQEShuffleRead(coalesced)
```

and reduced the effective downstream partition count.

## Supporting UI metrics

Query 11:

| Metric                                | Evidence   |
| ------------------------------------- | ---------- |
| Initial shuffle partitions            | 64         |
| Coalesced partitions                  | 2          |
| Shuffle records                       | ~1,000,002 |
| Shuffle bytes                         | ~9.2 MiB   |
| Remote bytes                          | 0          |
| Spill                                 | 0          |
| Final output groups                   | 1,000,001  |
| WholeStageCodegen maximum             | **602 ms** |
| Stage                                 | **33.0**   |
| Task                                  | **438**    |
| Final aggregation build maximum       | **494 ms** |
| Stage                                 | **33.0**   |
| Task                                  | **439**    |
| Pre-shuffle aggregation build maximum | **270 ms** |
| Stage                                 | **31.0**   |
| Task                                  | **435**    |

Again, Query 11 is a supporting `show(10)` action and should not replace the console benchmark of **0.84 s**.

---

# 9. Critical AQE Insight

It is important not to say:

> "AQE fixed the aggregation skew."

That statement is too broad.

What the evidence actually shows is:

```text
AQE ON
   ↓
post-shuffle statistics collected
   ↓
shuffle partitions found relatively small
   ↓
64 partitions coalesced to 2
   ↓
fewer downstream tasks
```

The experiment did **not** show AQE splitting:

```text
HOT_CUSTOMER
```

into multiple independent aggregation reducers.

---

# 10. Aggregation Skew vs Join Skew

This distinction is critical.

Spark has AQE functionality specifically designed for **skewed joins**.

A skewed join partition can be detected and split so that a large partition does not become a single oversized join task.

That mechanism should not be confused with generic:

```text
groupBy(customer_id)
```

aggregation skew.

In this experiment, the configuration:

```text
spark.sql.adaptive.skewJoin.enabled
```

is about **joins**.

It is not a general:

```text
"split any hot aggregation key"
```

switch.

Therefore:

> AQE should not be described as a universal solution for aggregation-key skew.

---

# 11. Experiment D — 80% Hot Key + AQE ON + Salting

## Purpose

D tests a manual technique commonly used when a hot aggregation key genuinely creates a reducer bottleneck:

> **Salting**

The experiment used:

```text
saltBuckets = 16
```

The salt was generated using:

```text
pmod(
    xxhash64(product_id, region_id),
    16
)
```

The aggregation was transformed from:

```text
groupBy(customer_id)
```

to:

```text
groupBy(customer_id, salt)
```

followed by a second aggregation:

```text
groupBy(customer_id)
```

---

# 12. Salted Physical Plan

The final physical plan clearly showed the additional processing:

```text
AdaptiveSparkPlan
+- HashAggregate(
    keys=[customer_id],
    functions=[sum(partial_count), sum(partial_amount)]
)
   +- Exchange hashpartitioning(customer_id, 64)
      +- HashAggregate(
          keys=[customer_id],
          functions=[partial_sum(partial_count),
                     partial_sum(partial_amount)]
      )
         +- HashAggregate(
             keys=[customer_id, salt],
             functions=[count(1), sum(amount)]
         )
            +- Exchange hashpartitioning(customer_id, salt, 64)
               +- HashAggregate(
                   keys=[customer_id, salt],
                   functions=[partial_count(1),
                              partial_sum(amount)]
               )
```

This is the key structural change.

The salted version introduced:

1. A salt column
2. A first partial aggregation
3. A shuffle by `(customer_id, salt)`
4. A first final aggregation by `(customer_id, salt)`
5. A second partial aggregation by `customer_id`
6. A second shuffle by `customer_id`
7. A second final aggregation

Therefore salting is **not free**.

---

# 13. Experiment D Result

```text
Aggregation result groups: 1,000,001
Aggregation execution time: 1.50 seconds
```

Compared with:

```text
B — AQE OFF, no salting = 1.50 s
C — AQE ON, no salting  = 0.84 s
D — AQE ON, salting     = 1.50 s
```

## Relative comparison

Using the measured benchmark values:

```text
B → C
1.50 s → 0.84 s
≈ 44% lower measured execution time
```

```text
B → D
1.50 s → 1.50 s
≈ no improvement
```

```text
C → D
0.84 s → 1.50 s
≈ 79% slower than C
```

These percentages are specific to this run and workload. They should not be generalized into universal Spark performance guarantees.

---

# 14. Spark UI Evidence for D

Query 15 was the supporting `show(10)` action and definitively showed the salted aggregation.

The final plan contained:

```text
salt = pmod(xxhash64(product_id, region_id, 42), 16)
```

followed by:

```text
Exchange hashpartitioning(customer_id, salt, 64)
```

and later:

```text
Exchange hashpartitioning(customer_id, 64)
```

This proves that salting introduced **two shuffle boundaries**.

## First salted shuffle

Observed:

* Initial partitions: **64**
* AQE coalesced: **2**
* Partition data: approximately **53.4 MiB**
* Shuffle bytes written: approximately **13.4 MiB**
* Shuffle records: approximately **1,000,032**
* Remote reads: 0
* Spill: 0

## Second shuffle

Observed:

* Initial partitions: **64**
* AQE coalesced: **2**
* Partition data: approximately **45.8 MiB**
* Shuffle bytes written: approximately **11.0 MiB**
* Shuffle records: approximately **1,000,002**
* Remote reads: 0
* Spill: 0

## Maximum UI metrics

Query 15:

| Metric                         |    Maximum |    Stage |    Task |
| ------------------------------ | ---------: | -------: | ------: |
| WholeStageCodegen (2)          | **810 ms** | **49.0** | **467** |
| First salted aggregation build | **520 ms** | **49.0** | **467** |
| Second aggregation build       |     376 ms |     52.0 |     468 |
| Another aggregation build      |     364 ms |     47.0 |     463 |
| Shuffle write time             |     122 ms |     49.0 |     467 |

The **810 ms WholeStageCodegen (2)** value was the largest explicit metric in that supporting UI action.

The maximum aggregation-build metric was:

```text
Stage 49.0
Task 467
520 ms
```

Again, these are UI metrics for the supporting `show(10)` action, not the authoritative D benchmark time.

---

# 15. Why Salting Did Not Help Here

This is one of the most valuable lessons from the experiment.

The workload was:

```text
COUNT(*)
SUM(amount)
GROUP BY customer_id
```

Spark already performs partial aggregation before the shuffle:

```text
4,000,000 hot-key input rows
             ↓
      local HashAggregate
             ↓
       much smaller state
             ↓
          shuffle
```

Therefore the hot customer's 4 million rows do not necessarily remain 4 million independent shuffle records.

Salting changes the workload into something more like:

```text
Input
  ↓
Partial HashAggregate(customer_id, salt)
  ↓
Shuffle #1
  ↓
HashAggregate(customer_id, salt)
  ↓
Partial HashAggregate(customer_id)
  ↓
Shuffle #2
  ↓
Final HashAggregate(customer_id)
```

For this particular COUNT + SUM workload, the additional work outweighed any potential benefit.

Therefore:

> **Salting should not be applied automatically just because the input is skewed.**

First prove that the skew creates an actual downstream bottleneck.

---

# 16. The Three Types of Skew You Must Distinguish

## 16.1 Input Skew

Example:

```text
HOT_CUSTOMER = 4,000,000 rows
Other customers = 1,000,000 rows
```

This experiment definitely has input/key skew.

---

## 16.2 Shuffle Skew

Shuffle skew means the records or bytes written to different shuffle partitions are highly uneven.

For example:

```text
Partition 0 → 100 KiB
Partition 1 → 110 KiB
Partition 2 → 105 KiB
...
Partition 17 → 800 MiB
```

That would be a serious shuffle skew signal.

The experiment did not show this kind of catastrophic shuffle imbalance.

---

## 16.3 Reducer / Task Skew

Reducer skew means one or a few downstream tasks take dramatically longer than the others.

Example:

```text
Task 0 → 220 ms
Task 1 → 240 ms
Task 2 → 230 ms
...
Task 37 → 45 seconds
```

That is the kind of evidence required before confidently saying:

> "This query has a reducer skew problem."

The experiments did not provide evidence of such a catastrophic reducer imbalance for the COUNT + SUM aggregation.

---

# 17. Why the Hot Key Did Not Automatically Become a Huge Shuffle Partition

Consider:

```text
4,000,000 rows
customer_id = HOT_CUSTOMER
```

Without partial aggregation, those rows could potentially produce an enormous shuffle payload for one key.

But Spark performs:

```text
HashAggregate
```

before the shuffle.

Conceptually:

```text
Task 1:
HOT_CUSTOMER → count=500,000, sum=...

Task 2:
HOT_CUSTOMER → count=500,000, sum=...

Task 3:
HOT_CUSTOMER → count=500,000, sum=...

...
```

Only the partial states need to move across the shuffle boundary.

The final aggregation then combines those partial states.

This is why:

> **Aggregation skew can behave very differently from join skew.**

---

# 18. AQE Evidence Summary

The AQE experiments repeatedly showed:

```text
64 initial shuffle partitions
            ↓
runtime statistics
            ↓
AQE coalescing
            ↓
2 effective downstream partitions
```

The physical plans contained:

```text
AQEShuffleRead(coalesced)
```

This is strong evidence that AQE was active and adapting the post-shuffle execution.

However:

```text
AQEShuffleRead(coalesced)
```

means partition coalescing.

It does not mean:

```text
HOT_CUSTOMER was split across multiple aggregation reducers
```

Those are different mechanisms.

---

# 19. Benchmark Summary

| Experiment | AQE | Salt |       Time | Main Observation                           |
| ---------- | --: | ---: | ---------: | ------------------------------------------ |
| A          | OFF |   No |   Baseline | Uniform distribution                       |
| B          | OFF |   No | **1.50 s** | Partial aggregation limits shuffle impact  |
| C          |  ON |   No | **0.84 s** | AQE coalesced 64 → 2                       |
| D          |  ON |  Yes | **1.50 s** | Extra shuffle/aggregation cost; no benefit |

### Important benchmark qualification

The timings are measured on a local/single-node experimental environment.

They are useful for demonstrating Spark execution behavior, but they are **not production capacity benchmarks**.

A production conclusion should be based on:

* multiple repetitions
* cluster-level measurements
* representative data volume
* realistic executor memory
* realistic partition sizing
* GC behavior
* spill behavior
* network shuffle
* concurrent workloads

---

# 20. What We Can and Cannot Conclude

## We can conclude

### 1. The input was genuinely highly skewed

80% of rows belonged to one customer.

### 2. Partial aggregation materially changes the effect of aggregation-key skew

The physical plan clearly contains:

```text
partial_count
partial_sum
```

before the shuffle.

### 3. AQE was active

The final plans contained:

```text
AdaptiveSparkPlan
AQEShuffleRead(coalesced)
```

### 4. AQE coalesced shuffle partitions

Observed:

```text
64 → 2
```

### 5. AQE did not demonstrate generic hot-key aggregation splitting

There was no evidence that the hot customer was split across multiple aggregation reducers.

### 6. Salting added substantial execution complexity

The salted plan contained two shuffle boundaries.

### 7. Salting did not improve this workload

Measured D:

```text
1.50 s
```

which was the same as B and substantially slower than C.

---

## We cannot conclude

We cannot conclude that:

* AQE always improves aggregation performance by 44%
* AQE never helps aggregation skew
* salting is ineffective
* salting should never be used
* 16 salt buckets is optimal or universally bad
* single-node timings represent production cluster performance

Those conclusions require broader experiments.

---

# 21. When Salting Can Actually Be Useful

Salting becomes interesting when there is evidence that:

```text
one aggregation key
        ↓
creates a genuinely oversized state/shuffle/reducer workload
        ↓
partial aggregation is insufficient
        ↓
one task becomes a straggler
```

Examples can include aggregations involving:

* very large aggregation state
* high-cardinality distinct operations
* large sets/maps
* workloads where local aggregation cannot sufficiently reduce state
* pathological hot keys combined with expensive aggregation logic

In those cases, splitting one logical key into multiple salted keys can distribute the workload.

But salting introduces:

```text
additional aggregation
additional shuffle
additional merge work
additional complexity
```

Therefore it should be justified by measurements.

---

# 22. Production Decision Framework

When investigating an aggregation that appears slow:

## Step 1 — Check the physical plan

Look for:

```text
HashAggregate
Exchange
HashAggregate
```

and determine whether partial aggregation exists.

---

## Step 2 — Inspect shuffle distribution

Check:

* shuffle bytes per partition
* records per partition
* task duration distribution
* max task versus median task
* spill
* memory pressure

---

## Step 3 — Determine the actual bottleneck

Ask:

```text
Is the input skewed?
```

then:

```text
Is the shuffle skewed?
```

then:

```text
Are reducers/tasks skewed?
```

Do not treat these as interchangeable.

---

## Step 4 — Check AQE behavior

Look for:

```text
AdaptiveSparkPlan
AQEShuffleRead
coalesced
```

and inspect the before/after partition counts.

---

## Step 5 — Only then consider salting

If there is demonstrable hot-key reducer pressure:

```text
hot key
  ↓
large state
  ↓
large partition
  ↓
straggler
```

then test salting.

Do not salt merely because a data-quality report says:

```text
80% of rows belong to one key
```

---

# 23. Interview-Level Explanation

A strong interview answer would be:

> "For aggregations, I first distinguish input-key skew from actual shuffle or reducer skew. Spark performs partial HashAggregate before the shuffle, so a hot key does not necessarily create a huge shuffle partition. AQE can coalesce small post-shuffle partitions and has specific skew handling for joins, but it should not be described as a generic hot-key aggregation splitter. If I prove that a hot aggregation key creates a reducer bottleneck that partial aggregation cannot address, I would consider salting. But salting adds another aggregation and shuffle, so I would validate the trade-off using Spark UI metrics rather than applying it blindly."

---

# 24. Evidence-Based Architecture View

The module demonstrates three increasingly sophisticated execution strategies.

### B — Static execution

```text
Input
  ↓
Partial HashAggregate
  ↓
Exchange × 64
  ↓
Final HashAggregate
```

### C — AQE execution

```text
Input
  ↓
Partial HashAggregate
  ↓
Exchange × 64
  ↓
Runtime statistics
  ↓
AQE coalescing
  ↓
AQEShuffleRead
  ↓
Final HashAggregate
```

### D — Salted execution

```text
Input
  ↓
Partial Aggregate(customer_id, salt)
  ↓
Exchange(customer_id, salt)
  ↓
Aggregate(customer_id, salt)
  ↓
Partial Aggregate(customer_id)
  ↓
Exchange(customer_id)
  ↓
Final Aggregate(customer_id)
```

This makes the cost/benefit trade-off visible directly in the physical plan.

---

# 25. Key Spark Internals Takeaways

## Takeaway 1

**Aggregation skew is not automatically reducer skew.**

---

## Takeaway 2

**Partial aggregation can dramatically reduce the amount of data crossing the shuffle boundary.**

---

## Takeaway 3

**AQE is not a magic "fix all skew" switch.**

---

## Takeaway 4

**`spark.sql.adaptive.skewJoin.enabled` is about skewed joins, not generic aggregation hot keys.**

---

## Takeaway 5

**`AQEShuffleRead(coalesced)` demonstrates partition coalescing, not hot-key splitting.**

---

## Takeaway 6

**Salting is a deliberate algorithmic change, not a configuration switch.**

---

## Takeaway 7

**Every optimization must be validated against the physical plan and Spark UI.**

---

## Takeaway 8

**A slower optimized plan can be more informative than a faster one.**

Experiment D is particularly valuable because it demonstrates that a commonly recommended technique can add overhead when the original aggregation is already efficient.

---

# 26. Final Conclusion

The most important lesson from Module 1.7.10 is:

> **Do not optimize "skew" as a generic problem. Identify exactly where the skew manifests in the execution pipeline.**

For this workload:

```text
80% input-key skew
        ↓
Partial HashAggregate
        ↓
manageable shuffle
        ↓
no catastrophic reducer skew
```

AQE then observed the relatively small shuffle and coalesced:

```text
64 → 2
```

The measured benchmark improved from:

```text
B: 1.50 s
C: 0.84 s
```

but the evidence indicates that the visible AQE behavior was primarily **post-shuffle partition coalescing**, not automatic splitting of the hot aggregation key.

Manual salting produced:

```text
B: 1.50 s
C: 0.84 s
D: 1.50 s
```

and introduced an additional aggregation/shuffle pipeline.

Therefore, for this COUNT + SUM workload:

> **Salting was unnecessary and added overhead.**

The production engineering lesson is not:

> "Never use salting."

It is:

> **Use salting only when measurements demonstrate a genuine hot-key aggregation bottleneck that the existing partial aggregation and AQE behavior cannot adequately handle.**

---

# 27. Module Completion Checklist

* [x] Understand aggregation-key skew
* [x] Establish uniform baseline
* [x] Create extreme hot-key skew
* [x] Compare AQE OFF vs ON
* [x] Inspect physical plans
* [x] Validate `AdaptiveSparkPlan`
* [x] Validate `AQEShuffleRead(coalesced)`
* [x] Observe 64 → 2 partition coalescing
* [x] Distinguish aggregation skew from join skew
* [x] Implement 16-way salting
* [x] Inspect salted physical plan
* [x] Observe additional shuffle boundary
* [x] Compare benchmark timings
* [x] Validate correctness
* [x] Inspect Spark UI metrics
* [x] Identify Stage/Task for maximum UI metrics
* [x] Derive production implications
* [x] Formulate interview-level explanation

**Module 1.7.10 is complete.**

---

# 28. Next Module

The next authoritative roadmap item is:

**Module 1.7.11 — Aggregation Strategy Comparison**

The purpose is to compare Spark aggregation strategies and understand when Spark chooses different physical execution approaches, rather than continuing to tune the skew experiment itself.

```

**Recommended filename:** `Module-1.7.10-AQE-Aggregation-Skew.md`

One small note: I deliberately kept the **actual benchmark timings separate from Spark UI timings**. This is important because several UI screenshots were from supporting `show(10)`/distribution actions rather than the exact timed `.count()` benchmark.
```
