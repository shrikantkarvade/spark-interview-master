# Module 1.8.3 — Shuffle Read/Write Performance

## 1. Objective

Module 1.8.2 asked *how does shuffle partition sizing affect execution?*
This module asks a different question: *what actually happens to data during
Spark shuffle, and how do shuffle configuration and workload shape the
read/write I/O?*

The goal is to connect four layers of evidence for each experiment case:

```
Configuration → Physical Plan → Stage → Task → Shuffle Metrics → Runtime
```

rather than optimizing from runtime alone.

## 2. Controlled dataset

Held constant across every experiment in the primary matrix:

| Parameter | Value |
|---|---|
| Input rows | 10,000,000 |
| Distinct keys (`customer_id`) | 1,000,000 |
| Input partitions | 6 |
| Cache | `input.cache()` before the matrix runs |
| AQE | Disabled for the primary matrix (A–F); one dedicated validation case (G) |

Run command:

```bash
./gradlew runShuffleReadWritePerformanceExperiment --args="10000000 1000000 6 --ui-pause=300"
```

## 3. Fix applied before this run

The first attempt at this experiment failed before any workload executed:

```
org.apache.spark.SparkNoSuchElementException: [SQL_CONF_NOT_FOUND]
The SQL config "spark.shuffle.compress" cannot be found.
```

**Root cause:** `spark.conf.get("spark.shuffle.compress")` uses
`RuntimeConfig`, which is backed by `SQLConf`. `SQLConf` only resolves
`spark.sql.*`-registered configs (or arbitrary keys explicitly set via
`spark.conf.set(...)` in-session). `spark.shuffle.compress` and
`spark.shuffle.spill.compress` are **core Spark configs**
(`org.apache.spark.internal.config`), never registered with `SQLConf`, so
`spark.conf.get` cannot resolve them regardless of when it's called — this
was not a timing/initialization-order issue.

**Fix:** read both keys from the underlying `SparkConf` instead:

```scala
spark.sparkContext.getConf.get("spark.shuffle.compress", "true")
spark.sparkContext.getConf.get("spark.shuffle.spill.compress", "true")
```

After the fix, both configs printed `true` in every experiment case
(Spark's own defaults), and the full matrix executed to completion.

## 4. Experiment matrix and results

| Case | Description | Shuffle Partitions | AQE | Runtime |
|---|---|---:|:---:|---:|
| A | Baseline, 1M-key count, same partition count as input | 6 | off | 2.877 s |
| B | Same aggregation, 4× shuffle partitions | 24 | off | 2.491 s |
| C | Same aggregation, ~11× shuffle partitions | 64 | off | 2.761 s |
| D | Wider aggregation: `count(1)` + `sum(amount)` | 24 | off | 2.157 s |
| E | Two-key grouping (`customer_id`, `product_id`) | 24 | off | 2.638 s |
| F | Two-key grouping, 64 partitions | 64 | off | 2.871 s |
| G | Two-key grouping, AQE validation | 64→2 (coalesced) | on | 2.619 s |

## 5. Shuffle I/O summary (from task-level listener + Spark UI cross-check)

| Case | Shuffle Read = Write | Read Records | Local Blocks | Local % | Spill |
|---|---:|---:|---:|---:|---:|
| A | 28.79 MiB | 6,000,006 | 42 | 100% | 0 B |
| B | 28.90 MiB | 6,000,024 | 168 | 100% | 0 B |
| C | 29.24 MiB | 6,000,064 | 448 | 100% | 0 B |
| D | 28.90 MiB | 6,000,024 | 168 | 100% | 0 B |
| E | 60.55 MiB | 6,000,024 | 168 | 100% | 0 B |
| F | 61.56 MiB | 6,000,064 | 448 | 100% | 0 B |
| G | 61.56 MiB | 6,000,002 | 14 | 100% | 0 B |

Every case's shuffle write bytes on the map stage exactly equal shuffle read
bytes on the reduce stage (cross-checked against the Stages tab — e.g. Stage
8 writes 29.2 MiB, Stage 9 reads 29.2 MiB) — no data loss and no
double-counting anywhere in the pipeline. Local-only reads throughout,
consistent with `local[2]` single-executor execution; this run does not
exercise remote shuffle fetch behavior.

## 6. Key findings

### 6.1 Runtime does not track shuffle volume (A vs. B vs. C)

A, B, and C move essentially the same shuffle bytes (28.8–29.2 MiB) — the
partition count changes how that volume is *sliced* (6 vs. 24 vs. 64 blocks
per task set) but not how much total data crosses the shuffle boundary.
Runtime is non-monotonic across the sweep (2.877 s → 2.491 s → 2.761 s):
more partitions did not reliably mean faster or slower execution here at
this data scale. This directly answers production question #14/#15 from the
experiment's own checklist — **the runtime differences are not explained by
shuffle I/O volume**, and are more consistent with per-task scheduling
overhead than with data movement.

### 6.2 The apparent "straggler" task is a warm-up artifact, not skew

Every one of A–F shows the same shape on the map-side stage: task 0/1 (the
first two dispatched, running `PROCESS_LOCAL`) take 400–800+ ms while tasks
2–5 in the same stage finish near-instantly once codegen/JIT is warm. Since
the input is uniformly distributed `spark.range` data with exactly even
partitioning, this is **not data skew** — it's the one-time cost of code
generation and column decompression absorbed by the first task(s) against
the cached `InMemoryTableScan`, amortized away for the rest of the stage.
This is confirmed by the Executors summary: **GC time was 0.2 s out of 54 s
total task time (~0.4%)** across the entire 264-task run, and peak execution
memory (320 MiB on-heap) stayed well under the 2.2 GiB budget with zero
spill anywhere — ruling out GC pressure or memory contention as the cause.

### 6.3 Two-key grouping roughly doubles shuffle volume (E/F vs. B/C)

Adding `product_id` to the grouping key (E, F) raises shuffle read/write
from ~29 MiB to ~61 MiB versus the equivalent single-key cases (B, C) at the
same partition counts — consistent with carrying an extra grouping column
through the partial-aggregation output. Task counts and physical plan shape
(`Exchange hashpartitioning(customer_id, product_id, N)`) otherwise mirror
the single-key cases exactly.

### 6.4 `count` vs. `count + sum` (D) shows no meaningful shuffle-side cost

D (wider aggregation: `count(1)` + `sum(amount)`) shows essentially
identical shuffle bytes and read/write records to B (24-partition, single
aggregate) — 28.90 MiB in both cases. Adding a second aggregate function
over an already-scanned column costs negligible extra shuffle I/O; the
`amount` column is read once during the `InMemoryTableScan`
(`Output [2]: [customer_id#3L, amount#5]` vs. `[customer_id#3L]` in B) and
partially pre-aggregated before the exchange, same as `count` alone.

### 6.5 AQE coalescing concentrates shuffle read onto fewer tasks (G)

G configures 64 shuffle partitions but AQE coalesces the post-shuffle read
down to **2 effective partitions** (confirmed via
`ShufflePartitionsUtil: advisory target size: 67108864, actual target size:
34654448`, and `AQEShuffleRead ... number of coalesced partitions: 2` in
the physical plan). The result: the same 61.56 MiB of shuffle data that was
spread across 64 small reads in F (each ~1 MiB) is instead concentrated into
just 2 tasks reading ~30.8 MiB each. Task-level imbalance indicators reflect
this directly — G's "max read / average read" ratio (4.50×) is the highest
in the whole matrix, driven by AQE deliberately merging small partitions
rather than by any actual data skew.

**Three stages (21, 23, 24) appear as "Skipped" in G's Stages tab.** This is
DAGScheduler stage-reuse: AQE recognized that a previously computed shuffle
map stage's output could be reused rather than recomputed, not a missed run
or an execution failure — direct, incidental evidence of AQE's internal
stage-reuse mechanism working as intended, distinct from the coalescing
behavior above.

## 7. Physical plan / Exchange verification

Every case's executed plan contains exactly the expected `Exchange` and
`HashAggregate` operators for its configuration, confirmed via the
`operatorStatus` check in each run:

| Case | Exchange | HashAggregate | SortAggregate | AQEShuffleRead |
|---|:---:|:---:|:---:|:---:|
| A–F | PRESENT | PRESENT | not present | not present |
| G | PRESENT | PRESENT | not present | not present* |

\* G's `AQEShuffleRead` operator appears in the **initial** adaptive plan
(`(9) AQEShuffleRead ... Arguments: coalesced`) but the code's
`operatorStatus` check runs against `queryExecution.executedPlan`, which at
the time of the check reflects an intermediate adaptive stage
(`isFinalPlan=false`) rather than the fully resolved final plan — the
coalescing is real and confirmed via the `ShufflePartitionsUtil` log line
and the final task counts, just not visible in this particular printed
check. Future iterations of this experiment should capture
`queryExecution.executedPlan` after forcing plan finalization (e.g. via an
additional trivial action) if this operator confirmation matters for a
given run.

## 8. Environment validation

- **264/264 tasks succeeded, 0 failed, 0 excluded executors** across the
  full run.
- **GC time: 0.2 s / 54 s total task time.**
- **Peak execution memory: 320 MiB on-heap, 0 B off-heap** (budget: 2.2 GiB).
- **0 B spill (memory or disk) in every experiment case.**
- **Aggregate shuffle read = shuffle write = 299.5 MiB** for the whole run
  (Executors summary), matching the sum of the seven experiments' individual
  shuffle volumes (28.79+28.90+29.24+28.90+60.55+61.56+61.56 ≈ 299.5 MiB) —
  an internal cross-check confirming the custom `SparkListener`'s
  aggregation matches Spark's own executor-level accounting.

## 9. Production engineering conclusions

1. **Shuffle partition count changed I/O granularity, not I/O volume**, for
   this dataset and workload shape — runtime differences across A/B/C are
   better explained by per-task scheduling/JIT overhead than by data moved.
2. **The single-worst-task pattern seen in every case is a warm-up
   artifact**, not skew — confirmed by uniform key distribution, negligible
   GC, and zero spill.
3. **Grouping-key width (single vs. two-key) has a direct, proportional
   effect on shuffle volume** (~2× for one extra key here); aggregate
   function count (count vs. count+sum) does not.
4. **AQE coalescing trades read parallelism for fewer, larger reads** —
   valuable when many shuffle partitions would otherwise produce
   many small blocks, but it concentrates I/O onto fewer tasks, which is
   worth watching when task-level memory or time budgets are tight.
5. This run produced **no spill, no skew, and no failed tasks** — the
   dataset and current partition ranges (6–64) stayed well within the
   cluster's memory and scheduling capacity. A future module could push
   partition counts or data skew intentionally to observe where these
   safety margins break down.
