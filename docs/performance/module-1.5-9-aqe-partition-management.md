# Module 1.5.9 — AQE Partition Management

## Engineering Question

How does Adaptive Query Execution dynamically manage shuffle
partitions after Spark has collected runtime statistics?

## Use Case

A production aggregation is initially planned with a relatively
large number of shuffle partitions. The actual shuffle output may
be much smaller than expected. Excessive downstream partitions can
create unnecessary tasks and scheduling overhead.

## Problem

Static Spark execution follows the configured shuffle partition
count.

With:

spark.sql.shuffle.partitions = 20

Spark initially plans:

HashAggregate
↓
Exchange hashpartitioning(key, 20)
↓
HashAggregate

But the actual shuffle output for this workload is tiny.

Can AQE detect the runtime shuffle size and coalesce shuffle-read
partitions?

## Dataset

Rows: 1,000,000
Input partitions: 2
Grouping keys: 100
Shuffle partitions configured: 20

Query:

spark.range(1000000)
.select($"id" % 100 as "key")
.groupBy($"key")
.count()

## Configuration

Master: local[2]

spark.sql.shuffle.partitions = 20

spark.sql.adaptive.coalescePartitions.enabled = true

spark.sql.adaptive.advisoryPartitionSizeInBytes = 1 MiB

## Experiment 1 — AQE Enabled

Configuration:

spark.sql.adaptive.enabled = true

### Initial Physical Plan

AdaptiveSparkPlan
+- HashAggregate
+- Exchange
+- HashAggregate
+- Project
+- Range

Exchange:

hashpartitioning(key, 20)

### Runtime Evidence

ShuffleMapStage:
- 2 tasks
- 2 output partitions

AQE observed:

advisory target size: 1048576
actual target size: 1048576

Final ResultStage:

- 1 output partition
- 1 task

Shuffle read:

2 non-empty blocks
4.0 KiB total
0 remote blocks

### Final Physical Plan

AdaptiveSparkPlan
+- == Final Plan ==
HashAggregate
+- AQEShuffleRead
+- ShuffleQueryStage
+- Exchange
+- HashAggregate
+- Project
+- Range

AQEShuffleRead:

Arguments: coalesced

AdaptiveSparkPlan:

Arguments: isFinalPlan=true

### Result

Result rows: 100
Execution time: 1323 ms

## Experiment 2 — AQE Disabled

Configuration:

spark.sql.adaptive.enabled = false
spark.sql.adaptive.coalescePartitions.enabled = false

### Physical Plan

HashAggregate
+- Exchange
+- HashAggregate
+- Project
+- Range

Exchange:

hashpartitioning(key, 20)

### Runtime Evidence

ShuffleMapStage:
- 2 tasks

ResultStage:
- 20 output partitions
- 20 tasks

### Result

Result rows: 100
Execution time: 450 ms

## Comparison

| Metric | AQE ON | AQE OFF |
|---|---:|---:|
| Configured shuffle partitions | 20 | 20 |
| Initial shuffle partitions | 20 | 20 |
| Final result-stage partitions | 1 | 20 |
| Final result-stage tasks | 1 | 20 |
| Result rows | 100 | 100 |
| Execution time | 1323 ms | 450 ms |

## What AQE Actually Changed

AQE did not remove the shuffle.

The Exchange remained:

hashpartitioning(key, 20)

Instead, AQE changed how the downstream shuffle output was
read.

The critical evidence is:

AQEShuffleRead
Arguments: coalesced

This demonstrates that AQE can use runtime shuffle statistics
to coalesce downstream shuffle-read partitions.

## Why Was AQE Slower?

AQE ON:

1323 ms

AQE OFF:

450 ms

AQE was approximately 2.94x slower in this experiment.

This should NOT be interpreted as evidence that AQE is slower
in production.

This workload is intentionally small:

- local[2]
- single machine
- 1 million in-memory rows
- only 100 groups
- approximately 4 KiB of shuffle output

The overhead of adaptive planning and partition management can
therefore dominate the actual computation.

This experiment demonstrates execution behavior, not a
production throughput benchmark.

## Important Distinction

Configured partitions:

20

does not necessarily mean:

20 useful downstream tasks.

With AQE enabled, Spark can inspect runtime shuffle statistics
and coalesce shuffle-read partitions.

Therefore:

Configured partition count ≠ final execution partition count.

## Production Interpretation

AQE partition coalescing is particularly useful when:

- the configured shuffle partition count is intentionally high
- actual shuffle output is much smaller
- many downstream partitions would otherwise process little data
- task scheduling overhead becomes significant

It is less meaningful for tiny workloads where adaptive overhead
can dominate execution.

## AQE Does Not Fix Every Partition Problem

AQE coalescing should not be confused with data-skew mitigation.

Coalescing addresses unnecessary/small shuffle partitions.

It does not automatically eliminate a fundamentally hot key.

That distinction was demonstrated separately in Module 1.5.7 and
Module 1.5.8.

## Relationship to Previous Experiments

Module 1.5.6:
Hash partitioning determines data distribution.

Module 1.5.7:
Uneven key distribution creates partition imbalance.

Module 1.5.8:
Salting can split a hot key across multiple distribution keys.

Module 1.5.9:
AQE can dynamically coalesce shuffle-read partitions based on
runtime statistics.

Together these demonstrate:

partitioning
→ distribution
→ imbalance/skew
→ mitigation
→ adaptive runtime management

## Production Decision Rules

1. Do not blindly increase shuffle partitions.
2. Do not blindly reduce shuffle partitions.
3. Inspect actual shuffle size and task distribution.
4. Use AQE to allow Spark to adapt where appropriate.
5. Diagnose data skew separately from small-partition overhead.
6. Treat local microbenchmarks as execution-behavior evidence,
   not production performance predictions.

## Interview Takeaways

### Q: Does AQE remove the shuffle?

No.

The Exchange remains. AQE changes the downstream execution
strategy and, in this experiment, coalesces shuffle-read
partitions.

### Q: What evidence proves AQE was active?

The final plan contains:

AdaptiveSparkPlan
AQEShuffleRead
Arguments: coalesced
isFinalPlan=true

### Q: Why were there 20 tasks without AQE?

Because the static physical plan retained the configured
20 shuffle partitions.

### Q: Why was AQE slower here?

The workload was tiny and local. Adaptive planning overhead
was larger than the potential benefit.

### Q: Does AQE solve data skew?

Not by partition coalescing alone.

Skewed hot keys require separate mitigation strategies such as
salting or skew-aware join handling.

## Final Engineering Lesson

Spark's configured partition count is a planning input, not
necessarily the final execution shape.

A production Spark engineer should reason about:

configured partitions
→ physical Exchange
→ runtime shuffle statistics
→ AQE decisions
→ final shuffle-read partitioning
→ actual tasks

The important skill is not simply knowing that AQE exists.

It is being able to prove what AQE changed by inspecting the
initial plan, final plan, scheduler stages, and runtime evidence.
