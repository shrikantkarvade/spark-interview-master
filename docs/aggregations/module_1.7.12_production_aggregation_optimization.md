# Module 1.7.12 — Production Aggregation Optimization

## 1. Module Overview

Module 1.7.12 is the production-engineering continuation of the aggregation work covered in Modules 1.7.10 and 1.7.11.

The objective is not simply to measure aggregation runtime.

The objective is to understand **where aggregation memory pressure originates, how aggregation cardinality and state width affect resource consumption, whether increasing shuffle partitions helps, and what Adaptive Query Execution (AQE) can and cannot solve**.

This module uses Spark UI evidence to validate:

* HashAggregate memory consumption
* Aggregation cardinality
* Aggregate state width
* Shuffle volume
* Shuffle write behavior
* Task-level memory pressure
* Spill behavior
* Hash map pressure
* Shuffle partition count
* AQE partition coalescing
* Relationship between WholeStageCodegen and HashAggregate metrics
* Production implications of aggregation design

The central production question is:

> **When a Spark aggregation experiences memory pressure, what is actually causing it, and which tuning lever addresses the real bottleneck?**

---

# 2. Learning Objectives

By completing this module, we should be able to:

1. Explain how aggregation cardinality affects HashAggregate memory.
2. Distinguish key-only aggregation from aggregation with additional state.
3. Understand how aggregate state width affects shuffle volume.
4. Explain why increasing `spark.sql.shuffle.partitions` does not necessarily reduce upstream HashAggregate memory.
5. Understand how AQE coalesces downstream shuffle partitions.
6. Explain why AQE does not retroactively reduce memory already consumed by an upstream aggregation map.
7. Identify whether an aggregation actually spills to memory or disk.
8. Distinguish WholeStageCodegen duration from the actual HashAggregate operator duration.
9. Interpret task-level Spark UI metrics correctly.
10. Translate Spark UI observations into production optimization decisions.

---

# 3. Experiment Class

```text
com.shrikant.spark.aggregations.AggregationMemorySpillExperiment
```

Supporting experiment:

```text
AggregationMemorySpillExperiment
```

This experiment is retained as the supporting memory/spill experiment for Module 1.7.12.

The primary focus of this module is **production aggregation optimization**, rather than merely attempting to force a spill.

---

# 4. Gradle Task

```text
runAggregationMemorySpillExperiment
```

Example:

```bash
./gradlew runAggregationMemorySpillExperiment --args="10000000 1000000 64 --ui-pause=60"
```

Parameters:

```text
rows
groups
shufflePartitions
--ui-pause=<seconds>
```

Example:

```bash
./gradlew runAggregationMemorySpillExperiment --args="10000000 1000000 64 --ui-pause=60"
```

Meaning:

```text
10,000,000 rows
1,000,000 groups
64 shuffle partitions
60-second Spark UI pause
```

---

# 5. Dataset

The experiment generates a synthetic transaction-style dataset.

Approximate schema:

```text
id
customer_id
amount
```

The base dataset is generated using:

```scala
spark.range(rows)
```

with deterministic expressions.

The grouping key is generated using:

```scala
pmod(id, groups)
```

This allows aggregation cardinality to be controlled independently of the input row count.

---

# 6. Why Controlled Cardinality Matters

Aggregation memory is strongly influenced by the number of distinct grouping keys.

For example:

```text
10K groups
100K groups
500K groups
1M groups
```

represent progressively larger aggregation maps.

The experiment therefore allows us to observe the transition from:

```text
small aggregation map
        ↓
medium aggregation map
        ↓
large aggregation map
        ↓
very large aggregation map
```

without changing the fundamental query structure.

---

# 7. Core Experiment Matrix

The main experiment matrix covered:

| Experiment | Groups | Aggregate | Shuffle Partitions | AQE |
| ---------- | -----: | --------- | -----------------: | --- |
| Q2         |    10K | Key only  |                 64 | OFF |
| Q3         |    10K | SUM       |                 64 | OFF |
| Q4         |   100K | Key only  |                 64 | OFF |
| Q5         |   100K | SUM       |                 64 | OFF |
| Q6         |   500K | Key only  |                 64 | OFF |
| Q7         |   500K | SUM       |                 64 | OFF |
| Q8         |     1M | Key only  |                 64 | OFF |
| Q9         |     1M | SUM       |                 64 | OFF |
| Q10        |     1M | Key only  |                128 | OFF |
| Q11        |     1M | SUM       |                128 | OFF |
| Q12        |     1M | Key only  |                 64 | ON  |
| Q13        |     1M | SUM       |                 64 | ON  |

The exact Spark UI query numbering may differ from the experiment labels depending on other Spark actions executed during the run.

Therefore, query identification must always be validated using the physical plan and aggregation expression.

---

# 8. Baseline — Global COUNT

The global `COUNT` experiment was used as a control case.

Physical structure:

```text
Range
  ↓
Project
  ↓
Partial HashAggregate
  ↓
Exchange SinglePartition
  ↓
Final HashAggregate
```

Important observation:

The WholeStageCodegen duration was considerably larger than the HashAggregate build time.

Therefore:

> WholeStageCodegen duration must not automatically be attributed entirely to HashAggregate.

This distinction is important when interpreting Spark UI metrics.

---

# 9. Cardinality Experiment

## 9.1 10K Groups — Key Only

Observed:

```text
Groups:                 10,000
Shuffle partitions:     64
Peak aggregation memory: 256 KiB
Shuffle:                ~132.8 KiB total
Shuffle records:        20,000
Spill:                  0
```

The aggregation map remained extremely small.

---

## 9.2 10K Groups — SUM

Observed:

```text
Groups:                 10,000
Aggregate:              SUM
Peak aggregation memory: 256 KiB
Shuffle:                ~203.6 KiB total
Shuffle records:        20,000
Spill:                  0
```

Compared with key-only aggregation, adding `SUM` increased the amount of state carried through the aggregation and therefore increased shuffle volume.

However, the observed peak memory remained within the same 256 KiB bucket.

---

# 10. 100K Groups

## 10.1 100K Groups — Key Only

Observed:

```text
Groups:                 100,000
Peak aggregation memory: ~66.0 MiB
Shuffle:                ~1.146 MiB total
Shuffle records:        200,000
Spill:                  0
```

This was a major increase compared with the 10K-group case.

The important observation is:

```text
10K groups      → 256 KiB
100K groups     → 66 MiB
```

Aggregation memory therefore does not grow as a simple linear-looking number in the Spark UI buckets.

Hash map implementation details, allocation behavior and internal memory structures matter.

---

## 10.2 100K Groups — SUM

Observed:

```text
Groups:                 100,000
Peak aggregation memory: ~66.0 MiB
Shuffle:                ~1.740 MiB total
Shuffle records:        200,000
Spill:                  0
```

The addition of `SUM` increased shuffle volume:

```text
~1.146 MiB
      ↓
~1.740 MiB
```

but did not materially increase the observed peak aggregation memory bucket.

This demonstrates an important distinction:

> Aggregate state width can increase shuffle footprint without necessarily producing a proportionally larger observed aggregation-memory metric.

---

# 11. 500K Groups

## 11.1 500K Groups — Key Only

Observed:

```text
Groups:                 500,000
Peak aggregation memory: ~80 MiB
Shuffle:                ~5.0 MiB total
Shuffle records:        1,000,000
Spill:                  0
```

The aggregation map became significantly larger.

---

## 11.2 500K Groups — SUM

Observed:

```text
Groups:                 500,000
Peak aggregation memory: ~80 MiB
Shuffle:                ~7.7 MiB total
Shuffle records:        1,000,000
Spill:                  0
```

Again, adding the aggregate value increased shuffle volume but did not produce a major change in the observed peak partial aggregation memory.

---

# 12. 1M Groups

## 12.1 1M Groups — Key Only

Observed:

```text
Groups:                 1,000,000
Shuffle partitions:     64
AQE:                    OFF

Peak partial aggregation memory: ~96 MiB
Shuffle records:                  2,000,000
Shuffle:                          ~9.8 MiB total
Spill:                            0
```

This represents the largest core cardinality test before changing shuffle partition count or AQE.

The progression was:

| Groups | Peak Partial Aggregation Memory |
| -----: | ------------------------------: |
|    10K |                         256 KiB |
|   100K |                         ~66 MiB |
|   500K |                         ~80 MiB |
|     1M |                         ~96 MiB |

This is the strongest result of the module.

---

# 13. 1M Groups — SUM

Observed:

```text
Groups:                 1,000,000
Shuffle partitions:     64
AQE:                    OFF

Peak partial aggregation memory: ~96 MiB
Shuffle:                          ~15.4 MiB total
Shuffle records:                  2,000,000
Spill:                            0
```

Compared with key-only aggregation:

```text
Key only:   ~9.8 MiB shuffle
SUM:        ~15.4 MiB shuffle
```

Therefore:

> Increasing aggregate state width increased shuffle footprint significantly while the observed partial aggregation memory remained around 96 MiB.

---

# 14. Shuffle Partition Experiment

The next experiment increased shuffle partitions:

```text
64 → 128
```

while maintaining:

```text
1M groups
```

and AQE disabled.

---

# 15. 1M Groups — Key Only — 128 Partitions

Observed:

```text
Groups:                 1,000,000
Shuffle partitions:     128
AQE:                    OFF

Peak partial aggregation memory: ~96 MiB
Shuffle records:                  2,000,000
Shuffle:                          ~9.9 MiB total
Spill:                            0
```

The critical observation:

```text
64 partitions   → ~96 MiB partial aggregation memory
128 partitions  → ~96 MiB partial aggregation memory
```

Increasing shuffle partitions did **not** reduce the upstream partial aggregation memory.

---

# 16. Why More Shuffle Partitions Did Not Solve Memory Pressure

The reason is architectural.

The execution structure is approximately:

```text
Input
  ↓
Project
  ↓
Partial HashAggregate
  ↓
Exchange
  ↓
Final HashAggregate
```

The partial HashAggregate happens **before** the shuffle Exchange.

Therefore:

```text
spark.sql.shuffle.partitions
```

controls the downstream shuffle partitioning.

It does not automatically divide the upstream aggregation map into 128 smaller maps.

Conceptually:

```text
Input partitions
       ↓
Partial HashAggregate
       ↓
   aggregation map
       ↓
     Exchange
       ↓
64 / 128 shuffle partitions
```

The memory pressure in the partial aggregation map occurs before the partition-count setting can help.

This is one of the most important production lessons from this module.

---

# 17. 1M Groups — SUM — 128 Partitions

Observed:

```text
Groups:                 1,000,000
Aggregate:              SUM
Shuffle partitions:     128
AQE:                    OFF

Peak partial aggregation memory: ~96 MiB
Shuffle:                          ~15.8 MiB total
Shuffle records:                  2,000,000
Spill:                            0
```

Again:

```text
64 partitions   → ~96 MiB
128 partitions  → ~96 MiB
```

The same memory behavior was observed for the SUM aggregation.

This reinforces the conclusion that simply increasing shuffle partitions is not a reliable solution for upstream aggregation-map memory pressure.

---

# 18. AQE Experiment

AQE was then enabled while keeping:

```text
1M groups
64 initial shuffle partitions
```

---

# 19. 1M Groups — Key Only — AQE ON

Observed:

```text
Initial shuffle partitions: 64
AQE:                        ON

Peak partial aggregation memory: ~96 MiB
Shuffle:                          ~9.8 MiB
Spill:                            0
```

AQE subsequently coalesced the downstream shuffle:

```text
64
 ↓
2 partitions
```

The Spark UI showed approximately:

```text
AQEShuffleRead
Partitions:             2
Coalesced partitions:   2
Partition data:         ~10.3 MiB
~5.1 MiB per partition
```

The downstream partitions were therefore reasonably balanced.

---

# 20. Critical AQE Finding

The most important result is:

```text
Partial HashAggregate memory:
~96 MiB
```

even though AQE later reduced:

```text
64 shuffle partitions
        ↓
2 downstream partitions
```

Therefore:

> AQE did not reduce the memory already consumed by the upstream partial HashAggregate.

The reason is the same architectural boundary:

```text
Partial HashAggregate
        ↓
Exchange
        ↓
AQE Shuffle Read
        ↓
Final HashAggregate
```

AQE operates on the shuffle boundary and downstream execution.

It cannot retroactively change how much memory the upstream aggregation map already needed.

---

# 21. 1M Groups — SUM — AQE ON

Observed:

```text
Initial shuffle partitions: 64
AQE:                        ON

Peak partial aggregation memory: ~96 MiB
Shuffle:                          ~15.4 MiB
AQE coalesced partitions:         2
Spill:                            0
```

Again:

```text
Partial aggregation memory ≈ 96 MiB
```

despite:

```text
64 → 2 downstream partitions
```

This confirms that the AQE observation is not limited to key-only aggregation.

---

# 22. Aggregation Memory Progression

The core progression is:

| Group Cardinality | Aggregate | Peak Partial Memory |    Shuffle |
| ----------------: | --------- | ------------------: | ---------: |
|               10K | Key only  |             256 KiB | ~132.8 KiB |
|               10K | SUM       |             256 KiB | ~203.6 KiB |
|              100K | Key only  |             ~66 MiB | ~1.146 MiB |
|              100K | SUM       |             ~66 MiB | ~1.740 MiB |
|              500K | Key only  |             ~80 MiB |   ~5.0 MiB |
|              500K | SUM       |             ~80 MiB |   ~7.7 MiB |
|                1M | Key only  |             ~96 MiB |   ~9.8 MiB |
|                1M | SUM       |             ~96 MiB |  ~15.4 MiB |

The most important pattern is:

```text
Higher cardinality
        ↓
Larger aggregation map
        ↓
Higher memory consumption
```

while:

```text
Wider aggregate state
        ↓
Higher shuffle footprint
```

without necessarily producing a proportional increase in the observed partial aggregation-memory metric.

---

# 23. Shuffle Partition Comparison

| Configuration  | Groups | Aggregate | Partial Memory |   Shuffle |
| -------------- | -----: | --------- | -------------: | --------: |
| 64 partitions  |     1M | Key only  |        ~96 MiB |  ~9.8 MiB |
| 128 partitions |     1M | Key only  |        ~96 MiB |  ~9.9 MiB |
| 64 partitions  |     1M | SUM       |        ~96 MiB | ~15.4 MiB |
| 128 partitions |     1M | SUM       |        ~96 MiB | ~15.8 MiB |

The experiment does **not** demonstrate that increasing shuffle partitions reduces upstream aggregation memory.

---

# 24. AQE Comparison

| Configuration | Initial Partitions | AQE Result | Partial Memory |
| ------------- | -----------------: | ---------: | -------------: |
| AQE OFF       |                 64 |         64 |        ~96 MiB |
| AQE ON        |                 64 |          2 |        ~96 MiB |

AQE successfully reduced downstream shuffle partition count.

However:

```text
AQE coalescing
      ≠
upstream aggregation-memory reduction
```

This distinction is critical.

---

# 25. Spill Validation

Across the observed core matrix:

```text
Spill (Memory): 0
Spill (Disk):   0
```

No actual spill was observed.

This is important because the experiment was designed to investigate memory and spill behavior, but the tested workload remained within available execution memory.

Therefore, we should **not claim that Spark spill behavior was demonstrated**.

The correct conclusion is:

> The experiment demonstrated substantial aggregation-memory growth and hash-map pressure as cardinality increased, but the tested configurations did not reach the threshold required to produce actual memory or disk spill.

This is a stronger engineering statement than artificially claiming a spill that was not observed.

---

# 26. HashAggregate vs WholeStageCodegen

Spark UI exposes metrics at different execution levels.

For example, one experiment may show:

```text
WholeStageCodegen:
1.4 s

HashAggregate:
1.0 s
```

These are not interchangeable.

WholeStageCodegen represents the generated execution pipeline containing multiple operators.

Therefore:

> A WholeStageCodegen maximum should not automatically be attributed entirely to HashAggregate.

When diagnosing aggregation performance, the analysis should separately identify:

```text
WholeStageCodegen duration
HashAggregate build time
Aggregation peak memory
Shuffle write
Shuffle bytes
Spill
Task duration
```

This prevents incorrect bottleneck attribution.

---

# 27. Task-Level Evidence

The Spark UI analysis should always identify the exact task associated with the maximum metric.

For example, one experiment showed:

```text
Partial HashAggregate build:
~1.1 s
Stage: 19
Task: 404
```

while:

```text
Peak aggregation memory:
~96 MiB
Stage: 19
Task: 403
```

These are different tasks.

Therefore, when documenting Spark UI results:

> Never assume that the task with the maximum build time is also the task with the maximum memory.

The correct production workflow is:

1. Identify the maximum metric.
2. Record the Stage ID.
3. Record the Task ID.
4. Inspect that task's metrics.
5. Compare it with other tasks.
6. Look for skew or outliers.

---

# 28. Skew Analysis

The grouping expression used in this experiment is deterministic:

```scala
pmod(id, groups)
```

This produces a regular distribution.

Therefore, the experiment is not a realistic skew workload.

The AQE coalesced partitions observed in the 1M-group tests were approximately balanced:

```text
~5.1 MiB per partition
```

and:

```text
~8.3 MiB per partition
```

for the corresponding SUM workload.

Therefore:

> No significant aggregation skew was demonstrated in this experiment.

This is important because memory pressure caused by high cardinality is different from memory pressure caused by skew.

---

# 29. High Cardinality vs Skew

These are two different production problems.

### High cardinality

```text
Many distinct keys
        ↓
Large HashAggregate map
        ↓
High executor/task memory
```

### Skew

```text
A small number of keys receive
disproportionately many records
        ↓
One or more tasks become much larger
        ↓
Task-level memory/runtime outliers
```

Possible solutions are therefore different.

High-cardinality problems may require:

* query redesign
* reducing grouping cardinality
* pre-aggregation
* partitioning strategy
* aggregation strategy changes
* executor memory tuning
* state-size reduction

Skew problems may require:

* AQE skew handling
* salting
* key decomposition
* skew-aware partitioning
* data-model changes

---

# 30. Production Interpretation

The experiment demonstrates an important diagnostic principle:

> **Tune the resource that is actually under pressure, not the resource whose name appears in the query configuration.**

For example, if the problem is:

```text
Partial HashAggregate
        ↓
96 MiB aggregation map
        ↓
memory pressure
```

blindly increasing:

```text
spark.sql.shuffle.partitions
```

may not solve the problem.

Likewise, enabling AQE may reduce:

```text
64 shuffle partitions
        ↓
2 partitions
```

but it does not necessarily reduce:

```text
upstream HashAggregate memory
```

that was already consumed.

---

# 31. Production Troubleshooting Decision Tree

When an aggregation is slow or memory-heavy:

```text
Aggregation problem
        |
        +--------------------------+
        |                          |
   Memory pressure             Runtime pressure
        |                          |
        ↓                          ↓
Check HashAggregate          Check task duration
memory                       WholeStageCodegen
        |                     Shuffle write
        ↓                     Shuffle read
Check cardinality            Partition count
        |
        ↓
Check state width
        |
        ↓
Check skew
        |
        ↓
Check spill
        |
        ↓
Check input partitioning
```

Then choose the tuning lever based on evidence.

---

# 32. What to Check in Spark UI

For a production aggregation investigation, inspect:

### SQL Details

```text
Physical plan
Exchange
HashAggregate
AQE
```

### Stage Details

```text
Input size
Output size
Shuffle read
Shuffle write
Task duration
```

### Task Details

```text
Peak execution memory
Spill memory
Spill disk
Task duration
Shuffle bytes
Records
```

### Operator Metrics

```text
HashAggregate build time
Average probes
Peak memory
Spill
Sort fallback
```

---

# 33. Important Production Lessons

## Lesson 1 — Cardinality is a primary aggregation-memory driver

The experiment showed:

```text
10K   → 256 KiB
100K  → ~66 MiB
500K  → ~80 MiB
1M    → ~96 MiB
```

Therefore, a high-cardinality `groupBy` can become a memory problem even when the input dataset itself is not extraordinarily large.

---

## Lesson 2 — More shuffle partitions do not automatically reduce aggregation-map memory

The experiment showed approximately:

```text
64 partitions   → ~96 MiB
128 partitions  → ~96 MiB
```

for 1M groups.

Therefore:

> `spark.sql.shuffle.partitions` is not a universal aggregation-memory tuning knob.

---

## Lesson 3 — AQE solves a different part of the problem

AQE successfully changed:

```text
64
 ↓
2
```

downstream shuffle partitions.

But:

```text
Partial HashAggregate memory
≈ 96 MiB
```

remained unchanged.

Therefore:

> AQE is excellent for adapting the post-shuffle execution plan, but it is not a retroactive fix for upstream aggregation-map memory.

---

## Lesson 4 — Aggregate state width affects shuffle

Adding:

```text
SUM(amount)
```

increased shuffle volume.

For 1M groups:

```text
Key only → ~9.8 MiB
SUM      → ~15.4 MiB
```

Therefore, wider aggregate state can increase network and shuffle overhead even when the observed aggregation-memory metric remains similar.

---

## Lesson 5 — No spill means no spill claim

The observed experiments had:

```text
Memory spill = 0
Disk spill   = 0
```

Therefore, the correct engineering conclusion is that the tested workload generated significant memory pressure but did not exceed the available execution-memory threshold.

---

## Lesson 6 — WholeStageCodegen is not the same as HashAggregate

A large WholeStageCodegen duration does not mean HashAggregate alone consumed that entire duration.

Operator-level metrics must be inspected separately.

---

## Lesson 7 — Always inspect the maximum task

Average metrics can hide the actual bottleneck.

Production diagnosis should identify:

```text
maximum metric
→ Stage ID
→ Task ID
→ task-level details
```

before drawing conclusions.

---

# 34. What This Experiment Does Not Prove

The experiment does **not** prove:

* that 128 shuffle partitions are faster than 64
* that AQE always improves aggregation runtime
* that AQE always coalesces to exactly 2 partitions
* that 96 MiB is a fixed memory requirement for 1M groups
* that SUM always increases memory by a particular amount
* that the tested workload will spill on a production cluster
* that the measured runtime is universally reproducible

Runtime can vary because of:

* JVM warm-up
* JIT compilation
* garbage collection
* OS scheduling
* machine load
* executor memory
* Spark configuration
* data source characteristics
* Spark version

Therefore, conclusions about runtime should be based on repeated measurements.

The strongest conclusions from this module are architectural and metric-based rather than single-run timing comparisons.

---

# 35. Production Optimization Strategy

When faced with a real production aggregation issue, use the following order.

## Step 1 — Determine cardinality

Estimate:

```text
number of input rows
number of distinct grouping keys
```

High-cardinality aggregation is a primary suspect when memory rises sharply.

---

## Step 2 — Determine aggregate state width

Compare:

```text
groupBy(key)
```

against:

```text
groupBy(key)
  .agg(
      count(...),
      sum(...),
      avg(...),
      min(...),
      max(...)
  )
```

More state means more data must be maintained and eventually shuffled.

---

## Step 3 — Check skew

Look for:

```text
one task significantly larger
one task significantly slower
one task with much larger memory
```

If the problem is skew, cardinality tuning alone may not solve it.

---

## Step 4 — Check spill

Inspect:

```text
Spill (Memory)
Spill (Disk)
```

If spill is occurring, investigate:

* executor memory
* execution-memory contention
* aggregation cardinality
* input partitioning
* aggregation strategy
* query design

---

## Step 5 — Check the Exchange boundary

Ask:

> Is the memory problem before or after the shuffle?

This is critical.

If the pressure is:

```text
before Exchange
```

then changing downstream shuffle partitions or relying solely on AQE may not solve it.

---

## Step 6 — Check AQE

AQE can help with:

* post-shuffle partition sizing
* small partition overhead
* skew handling in applicable scenarios
* adaptive execution decisions

But it should not be treated as a universal memory-pressure solution.

---

## Step 7 — Only then tune configuration

Possible configuration changes should be evidence-driven.

Examples include:

```text
spark.sql.shuffle.partitions
spark.sql.adaptive.enabled
spark.sql.adaptive.coalescePartitions.enabled
spark.sql.adaptive.skewJoin.enabled
```

and executor memory-related settings where appropriate.

Configuration changes should follow diagnosis rather than precede it.

---

# 36. Architecture-Level Mental Model

The most useful mental model from this module is:

```text
                    INPUT
                      |
                      v
                Project / Filter
                      |
                      v
             Partial HashAggregate
                      |
                +-----+-----+
                |           |
          aggregation       |
          map memory        |
                |           |
                v           |
             Exchange <-----+
                |
                v
        AQE Shuffle Read
                |
                v
        Final HashAggregate
                |
                v
              Result
```

The important boundary is:

```text
Partial HashAggregate
        |
        |  memory pressure happens here
        v
     Exchange
        |
        |  shuffle partition tuning / AQE
        v
AQE Shuffle Read
```

Therefore:

> **Upstream aggregation-map memory and downstream shuffle partitioning are related, but they are not the same resource problem.**

---

# 37. Module Conclusion

Module 1.7.12 demonstrates that production aggregation optimization requires understanding Spark's execution architecture rather than blindly changing configuration values.

The strongest evidence from the experiment is:

```text
10K groups
    ↓
256 KiB

100K groups
    ↓
~66 MiB

500K groups
    ↓
~80 MiB

1M groups
    ↓
~96 MiB
```

At 1M groups:

```text
64 shuffle partitions
        ↓
~96 MiB partial aggregation memory

128 shuffle partitions
        ↓
~96 MiB partial aggregation memory
```

And with AQE:

```text
64 initial partitions
        ↓
2 coalesced downstream partitions
```

while:

```text
partial aggregation memory
        ↓
still ~96 MiB
```

Therefore, the central production lesson is:

> **Aggregation memory pressure is primarily driven by the state maintained by the upstream aggregation operator. Increasing downstream shuffle partitions or enabling AQE does not automatically reduce that upstream memory requirement.**

For production systems, optimization should begin by identifying:

```text
cardinality
state width
skew
spill
input partitioning
aggregation strategy
Exchange boundaries
AQE behavior
```

and only then selecting the appropriate tuning strategy.

---

# 38. Final Takeaways

### Spark aggregation optimization checklist

```text
✓ Measure distinct grouping keys
✓ Inspect HashAggregate metrics
✓ Identify maximum Stage ID / Task ID
✓ Check peak execution memory
✓ Check memory spill
✓ Check disk spill
✓ Check shuffle bytes
✓ Check shuffle records
✓ Check task skew
✓ Inspect physical plan
✓ Identify Exchange boundaries
✓ Understand aggregate state width
✓ Test shuffle partition changes
✓ Test AQE separately
✓ Do not confuse WSCG with HashAggregate
✓ Do not claim spill without observing spill
✓ Do not attribute runtime changes causally from one run
✓ Tune based on the actual bottleneck
```

---

# 39. Module Status

```text
Module 1.7.12
Production Aggregation Optimization
```

Status:

**CORE MATRIX COMPLETE**

Covered:

```text
✓ Aggregation cardinality
✓ HashAggregate memory
✓ Aggregate state width
✓ Shuffle footprint
✓ 64 vs 128 shuffle partitions
✓ AQE ON vs OFF
✓ AQE partition coalescing
✓ Spill validation
✓ Task-level metric analysis
✓ Stage ID / Task ID mapping
✓ Skew interpretation
✓ WholeStageCodegen vs HashAggregate
✓ Production optimization principles
```

The observed workloads did not produce actual memory/disk spill, so spill should be treated as a **validated non-event in the core matrix**, not as a demonstrated spill scenario.

---

# 40. Relationship to Previous Modules

### Module 1.7.10

Focused on:

```text
AQE
Aggregation
Skew
```

### Module 1.7.11

Focused on:

```text
Aggregation strategy comparison
HashAggregate
Aggregation shape
Cardinality
Aggregate expressions
Execution plans
```

### Module 1.7.12

Focuses on:

```text
Production optimization
Memory pressure
Hash map growth
Spill validation
Shuffle partition tuning
AQE limitations
Task-level diagnosis
Production decision making
```

Together, the modules build the progression:

```text
Aggregation fundamentals
        ↓
Aggregation strategy
        ↓
AQE + skew
        ↓
Memory pressure
        ↓
Production optimization
```

This completes the current **Module 1.7 aggregation optimization learning sequence**.
