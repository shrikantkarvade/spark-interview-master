# Module 1.5.7 — Partition Imbalance and Data Skew

## Engineering Question

> What happens when a highly uneven key distribution is hash-partitioned across Spark partitions, and how does that imbalance affect parallelism and task execution?

This experiment investigates the relationship between:

* key distribution
* hash partitioning
* partition size
* parallelism
* task execution time
* data skew
* stragglers

The goal is to move beyond the statement **"data skew is bad"** and demonstrate exactly how skew becomes a physical execution problem.

---

# 1. Use Case

Consider a large transaction-processing pipeline partitioned by `customer_id`.

In a theoretically ideal workload, customer records would be distributed relatively evenly across Spark partitions.

Real production workloads are rarely that uniform.

For example:

* one customer may generate millions of transactions
* a small number of customers may generate a disproportionate amount of traffic
* the remaining customers may have relatively small volumes

If the data is hash-partitioned by `customer_id`, records belonging to the same key are directed to the same hash bucket.

That is useful for key-based operations.

However, if one or more high-frequency keys are mapped to the same partition, that partition can become dramatically larger than the others.

The result is:

```text
Uneven key distribution
        ↓
Hash partitioning
        ↓
Uneven partition sizes
        ↓
Uneven task workloads
        ↓
Straggler tasks
        ↓
Longer stage execution
```

This is the physical manifestation of data skew.

---

# 2. Problem

A common Spark optimization mistake is to look only at the number of partitions.

For example:

```scala
df.repartition(8, $"customer_id")
```

produces eight output partitions.

But eight partitions does **not** necessarily mean eight equally sized partitions.

The partition count answers:

> How many partitions exist?

It does not answer:

> How much data exists in each partition?

Two jobs may both have eight partitions while having radically different execution characteristics:

```text
Balanced:

P0  ████████████ 125K
P1  ████████████ 125K
P2  ████████████ 125K
P3  ████████████ 125K
P4  ████████████ 125K
P5  ████████████ 125K
P6  ████████████ 125K
P7  ████████████ 125K


Skewed:

P0  ██            114K
P1  ▏             18K
P2  ▏             11K
P3  █████         63K
P4  █████         64K
P5  █████████████████████████████████████████████████████████████████████████ 705K
P6  ▏             15K
P7  ▏             12K
```

In the second case, Spark still has eight partitions, but the work is nowhere near evenly distributed.

---

# 3. Experiment Design

The experiment deliberately creates a non-uniform key distribution and then hash-partitions the data into eight partitions.

## Spark Environment

```text
Apache Spark = 3.5.1
Scala        = 2.12.18
Java         = 17.0.20
Execution    = local[2]
Environment  = dev
```

The experiment uses the project's shared:

```text
EnterpriseSparkSession
```

configuration.

The local execution environment is important when interpreting the runtime numbers. There is no multi-node network in this experiment.

---

# 4. Dataset

The experiment generates:

```text
1,000,000 rows
```

using:

```scala
spark.range(1000000)
```

The input starts with:

```text
Input partitions = 2
Input rows       = 1000000
```

The key distribution is intentionally uneven.

The key-generation logic is:

```scala
val df =
  spark.range(1000000)
    .select(
      when($"id" < 500000, lit(0))
        .when($"id" < 700000, lit(1))
        .when($"id" < 800000, lit(2))
        .when($"id" < 850000, lit(3))
        .when($"id" < 900000, lit(4))
        .otherwise($"id" % 95 + 5)
        .as("distribution_key"),
      $"id"
    )
```

This produces the intended distribution:

|  Key | Approximate rows | Share |
| ---: | ---------------: | ----: |
|    0 |          500,000 |   50% |
|    1 |          200,000 |   20% |
|    2 |          100,000 |   10% |
|    3 |           50,000 |    5% |
|    4 |           50,000 |    5% |
| 5–99 |    100,000 total |   10% |

The first five keys therefore account for approximately 90% of the dataset.

The remaining 10% is spread across a long tail of keys.

---

# 5. Observed Key Distribution

The experiment confirmed the intended skew.

The Spark output included:

```text
+----------------+------+
|distribution_key|count |
+----------------+------+
|0               |500000|
|1               |200000|
|2               |100000|
|3               |50000 |
|4               |50000 |
|5               |1053  |
|6               |1053  |
|7               |1053  |
|8               |1053  |
|9               |1053  |
|10              |1053  |
|11              |1053  |
|12              |1053  |
|13              |1053  |
|14              |1053  |
|15              |1053  |
|16              |1053  |
|17              |1053  |
|18              |1053  |
|19              |1053  |
+----------------+------+
```

This is important because it proves that the partition imbalance is not accidental.

The input itself is skewed.

The dominant key has:

```text
500,000 rows
```

while the long-tail keys have only approximately:

```text
1,053 rows each
```

---

# 6. Hash Partitioning

The dataset is then repartitioned using:

```scala
val partitioned =
  df.repartition(8, $"distribution_key")
```

This introduces a shuffle.

The resulting physical plan contains:

```text
Exchange
Arguments: hashpartitioning(distribution_key#2L, 8),
           REPARTITION_BY_NUM
```

The important physical execution shape is:

```text
Range
  ↓
Project
  ↓
Exchange
  ↓
8 hash partitions
```

The `Exchange` is the shuffle boundary.

Spark must redistribute records according to the requested hash partitioning.

---

# 7. Physical Plan Evidence

The actual physical plan was:

```text
== Physical Plan ==
AdaptiveSparkPlan (4)
+- Exchange (3)
   +- Project (2)
      +- Range (1)

(1) Range
Output [1]: [id#0L]
Arguments: Range (0, 1000000, step=1, splits=Some(2))

(2) Project
Output [2]: [
  CASE
    WHEN (id#0L < 500000) THEN 0
    WHEN (id#0L < 700000) THEN 1
    WHEN (id#0L < 800000) THEN 2
    WHEN (id#0L < 850000) THEN 3
    WHEN (id#0L < 900000) THEN 4
    ELSE ((id#0L % 95) + 5)
  END AS distribution_key#2L,
  id#0L
]
Input [1]: [id#0L]

(3) Exchange
Input [2]: [distribution_key#2L, id#0L]
Arguments:
hashpartitioning(distribution_key#2L, 8),
REPARTITION_BY_NUM

(4) AdaptiveSparkPlan
Output [2]: [distribution_key#2L, id#0L]
Arguments: isFinalPlan=false
```

This plan tells us three important things.

### 7.1 The input has two partitions

The `Range` operator reports:

```text
splits=Some(2)
```

Therefore the upstream workload starts with two partitions.

### 7.2 Repartitioning introduces an Exchange

The key operation:

```scala
repartition(8, $"distribution_key")
```

requires:

```text
Exchange
```

because Spark must redistribute records according to the requested hash partitioning.

### 7.3 The requested distribution is hash-based

The physical plan explicitly reports:

```text
hashpartitioning(distribution_key, 8)
```

Therefore the experiment is measuring the effect of distributing a skewed key population across eight hash buckets.

---

# 8. Actual Partition Distribution

The most important result of the experiment is the actual number of rows in each output partition.

Spark produced:

```text
Partition 0 -> 113686 rows
Partition 1 -> 17893 rows
Partition 2 -> 10526 rows
Partition 3 -> 62631 rows
Partition 4 -> 63686 rows
Partition 5 -> 705263 rows
Partition 6 -> 14736 rows
Partition 7 -> 11579 rows
```

In table form:

| Partition |          Rows | % of Dataset |
| --------: | ------------: | -----------: |
|         0 |       113,686 |       11.37% |
|         1 |        17,893 |        1.79% |
|         2 |        10,526 |        1.05% |
|         3 |        62,631 |        6.26% |
|         4 |        63,686 |        6.37% |
|         5 |       705,263 |       70.53% |
|         6 |        14,736 |        1.47% |
|         7 |        11,579 |        1.16% |
| **Total** | **1,000,000** |     **100%** |

This is severe partition imbalance.

---

# 9. Quantifying the Imbalance

For eight perfectly balanced partitions, the theoretical average is:

```text
1,000,000 / 8
= 125,000 rows
```

The largest partition contains:

```text
705,263 rows
```

The smallest contains:

```text
10,526 rows
```

Therefore:

```text
max / min
=
705,263 / 10,526
≈ 67.0x
```

The largest partition is approximately:

```text
705,263 / 125,000
≈ 5.64x
```

the ideal average partition size.

Most importantly:

```text
Partition 5 = 70.53% of the entire dataset
```

So although Spark created eight partitions, approximately 70.5% of all rows ended up in a single partition.

That is the central result of Module 1.5.7.

---

# 10. Why This Happens

The important distinction is between:

```text
partition count
```

and:

```text
data distribution
```

The command:

```scala
df.repartition(8, $"distribution_key")
```

guarantees a requested eight-way hash partitioning scheme.

It does not guarantee:

```text
125,000 rows per partition
```

Hash partitioning is based on the partitioning key.

If key frequencies are highly uneven, the resulting hash buckets can also be highly uneven.

In this experiment:

```text
Key 0 → 500,000 rows
Key 1 → 200,000 rows
Key 2 → 100,000 rows
Key 3 → 50,000 rows
Key 4 → 50,000 rows
Keys 5–99 → ~100,000 rows total
```

The hash partitioner assigns those keys to eight partitions.

Several large keys can land in the same partition.

The result is:

```text
Highly skewed keys
        ↓
Hash partitioning
        ↓
Large hash bucket
        ↓
Large partition
```

The exact partition number receiving a heavy key is dependent on the hash-partitioning scheme and partition count.

Therefore:

> Partition 5 being the largest partition in this experiment is an observed result, not a universal property of key 0 or hash partitioning.

---

# 11. Partition Imbalance vs Data Skew

These terms are related but should not be treated as synonyms.

## Partition imbalance

Partition imbalance describes the physical result:

```text
Partitions contain significantly different amounts of data/work.
```

Our experiment clearly demonstrates this:

```text
10,526 rows
       vs
705,263 rows
```

## Data skew

Data skew describes the underlying distribution:

```text
Some key values occur much more frequently than others.
```

Our input contains:

```text
customer/key 0 → 500,000 rows
```

compared with approximately:

```text
1,053 rows per long-tail key
```

That is the source-level skew.

## Straggler

A straggler is a task that takes substantially longer than its peers.

The relationship is:

```text
Data skew
    ↓
Partition imbalance
    ↓
Uneven task workload
    ↓
Straggler task
```

This distinction is important when diagnosing production Spark jobs.

---

# 12. Task-Level Evidence

The hash-partitioning stage subsequently processed the eight output partitions.

Observed task durations included approximately:

```text
Partition 0 → 147 ms
Partition 1 → 109 ms
Partition 2 →  32 ms
Partition 3 →  37 ms
Partition 4 →  36 ms
Partition 5 → 150 ms
Partition 6 →  16 ms
Partition 7 →  17 ms
```

Partition 5 was both:

```text
the largest partition
```

and:

```text
the slowest observed partition
```

This is exactly the relationship we expect from skew.

The heaviest partition contained:

```text
705,263 rows
```

while partition 6 contained:

```text
14,736 rows
```

and partition 7 contained:

```text
11,579 rows
```

The largest partition therefore had dramatically more data to process.

### Important benchmark caveat

The exact task-time ratio should **not** be interpreted as a pure linear relationship between rows and execution time.

This experiment runs on:

```text
local[2]
```

on a single machine.

Task startup, JVM/code-generation effects, scheduling, and other local execution overheads influence these small runtime measurements.

The strong evidence is therefore the combination of:

1. highly skewed key frequencies
2. severe partition-size imbalance
3. the physical `hashpartitioning` exchange
4. the heavy partition being among the slowest tasks

rather than a claim that runtime scales exactly with row count.

---

# 13. Parallelism Does Not Mean Equal Work

This experiment demonstrates an important Spark performance principle:

> More partitions do not automatically mean better parallelism.

We have:

```text
8 partitions
```

but the distribution is:

```text
705K
114K
64K
63K
18K
15K
12K
11K
```

Only a small subset of the partitions carry substantial amounts of data.

Therefore, increasing the partition count alone would not necessarily solve the problem.

For example:

```scala
df.repartition(1000, $"distribution_key")
```

would create more hash buckets, but a very high-frequency key still has to belong somewhere.

If the skewed key cannot be split across multiple partitions for the operation being performed, the heavy key can continue to create a hot partition.

This is why skew requires more than simply increasing:

```text
spark.sql.shuffle.partitions
```

---

# 14. The Local Execution Caveat

The experiment was executed with:

```text
spark.master = local[2]
```

Therefore the Spark application ran on a single machine.

The Spark logs showed shuffle block fetches as:

```text
0 remote
```

This does **not** mean that the shuffle has no network cost in general.

It means that this particular local execution did not require cross-node network transfer.

On a real Spark cluster:

```text
Executor A
     ↓
Shuffle data
     ↓
Executor B
```

can introduce network transfer.

Therefore production impact can be significantly larger when skew causes:

* larger shuffle blocks
* uneven executor workload
* network hotspots
* executor memory pressure
* longer-running tasks
* downstream stage delays

---

# 15. Why the Slowest Task Matters

Spark stages complete according to the slowest required work.

Conceptually:

```text
Task 1 ────────┐
Task 2 ────────┤
Task 3 ────────┤
Task 4 ────────┤
Task 5 ────────────────┐
Task 6 ────────┤       │
Task 7 ────────┤       │
Task 8 ────────┘       ↓
                  Stage completion
```

Even if seven tasks finish quickly, one heavily skewed task can keep the stage alive.

This produces the familiar production symptom:

```text
99% of tasks completed
1 task still running
```

The remaining task may appear to be "stuck".

It may not be stuck.

It may simply be processing a disproportionately large partition.

This is one of the most important reasons to inspect the Spark UI's task-level distribution instead of looking only at total stage duration.

---

# 16. Why `repartition()` Cannot Automatically Fix Arbitrary Key Skew

It is tempting to solve the problem by simply calling:

```scala
df.repartition(8, $"distribution_key")
```

But that is exactly the operation being studied here.

The repartitioning creates the requested hash distribution.

It does not magically make the key frequencies uniform.

If:

```text
customer A = 500M records
customer B = 100 records
```

then a hash partitioner cannot split all records belonging to customer A across arbitrary hash buckets while simultaneously preserving the basic property that a key maps to a partition.

The key itself is the unit of distribution.

This creates an important trade-off:

```text
Key locality
     vs
Perfect workload balance
```

For key-based operations, preserving key locality is often necessary.

When a few keys dominate the dataset, specialized skew-handling strategies may be required.

---

# 17. AQE Considerations

The experiment ran with Adaptive Query Execution enabled through the project's development configuration.

The physical plan reported:

```text
AdaptiveSparkPlan
Arguments: isFinalPlan=false
```

The important observation is that enabling AQE did not make the skewed hash partition distribution disappear.

The resulting partition distribution remained:

```text
113,686
17,893
10,526
62,631
63,686
705,263
14,736
11,579
```

Therefore:

> AQE should not be interpreted as a universal automatic solution for arbitrary partition imbalance.

AQE provides several adaptive capabilities, including:

* coalescing small shuffle partitions
* changing certain join strategies
* skew handling for supported join scenarios

However, those capabilities are not equivalent to automatically redistributing every skewed key for every Spark operation.

This distinction matters in production architecture.

---

# 18. Relationship to Module 1.5.6

Module 1.5.6 used a different dataset with approximately uniform key frequency.

There, each of 100 keys occurred approximately:

```text
10,000 times
```

Yet the eight hash partitions were still somewhat uneven:

```text
70K – 170K rows
```

That demonstrated:

> Hash partitioning does not guarantee equal partition sizes even when key frequencies are uniform.

Module 1.5.7 goes one step further.

Here the input itself is deliberately skewed:

```text
500K
200K
100K
50K
50K
~1K per long-tail key
```

The resulting partition distribution becomes dramatically more uneven:

```text
10.5K – 705.3K
```

Therefore the two experiments establish two different concepts:

```text
Module 1.5.6
Uniform key frequency
        ↓
Hash bucket imbalance
```

versus:

```text
Module 1.5.7
Highly non-uniform key frequency
        ↓
Severe partition imbalance
        ↓
Potential stragglers
```

This distinction is important when diagnosing production problems.

---

# 19. Production Diagnosis

When a Spark job has one or a few very slow tasks, investigate in this order.

## Step 1 — Inspect partition sizes

Look for:

```text
max partition size
min partition size
median partition size
```

A large max/min ratio is a warning sign.

---

## Step 2 — Inspect key frequency

If the workload is key-based, identify whether a small number of keys dominate:

```text
customer_id
account_id
merchant_id
product_id
partition_key
```

For example:

```text
customer A → 45%
customer B → 18%
customer C → 10%
remaining customers → 27%
```

This is a strong skew signal.

---

## Step 3 — Inspect Spark UI task distribution

Compare:

* task duration
* input size
* shuffle read
* shuffle write
* records processed
* peak execution memory
* spill

A skewed stage frequently shows one or a few tasks with significantly larger shuffle/input sizes and longer durations.

---

## Step 4 — Inspect the physical plan

Look for:

```text
Exchange
hashpartitioning(...)
```

This tells you that Spark is redistributing data according to a partitioning scheme.

---

## Step 5 — Determine whether the skew is expected

Not every imbalance is a defect.

Some workloads naturally have hot keys.

The architectural question is:

> Is the resulting imbalance acceptable for the workload and SLA?

---

# 20. Production Mitigation Strategies

There is no universal skew fix.

The correct strategy depends on the operation and the business semantics.

### 20.1 Salt heavily skewed keys

Transform:

```text
customer_id
```

into something like:

```text
(customer_id, salt)
```

for selected hot keys.

This allows the workload associated with a hot key to be distributed across multiple partitions.

However, salting introduces additional complexity and often requires a corresponding strategy during joins or aggregation.

---

### 20.2 Separate hot keys from the normal path

If only a small number of keys are pathological:

```text
Normal keys → standard processing
Hot keys    → specialized processing
```

This can be cleaner than applying an expensive skew-handling mechanism to the entire dataset.

---

### 20.3 Broadcast small reference data

For joins where one side is genuinely small enough:

```text
large fact
    +
small dimension
```

a broadcast join can avoid repartitioning the large side.

This does not solve every form of skew, but it can remove an unnecessary shuffle for suitable join workloads.

---

### 20.4 Use AQE where applicable

AQE can help with certain adaptive execution scenarios.

However, do not treat:

```text
spark.sql.adaptive.enabled=true
```

as a blanket skew solution.

Validate the actual executed plan and task distribution.

---

### 20.5 Reconsider the partitioning key

Sometimes the chosen key is itself the problem.

Ask:

> Does this key provide the right balance for the workload?

A technically valid partitioning key may still be operationally poor.

---

### 20.6 Avoid blindly increasing partition count

Increasing:

```text
spark.sql.shuffle.partitions
```

can improve parallelism when partitions are simply too large.

It does not necessarily solve severe key skew.

Always determine whether the problem is:

```text
too little parallelism
```

or:

```text
uneven parallelism
```

These are different problems.

---

# 21. Partition Imbalance Is a Workload Problem

A common mistake is to think of partitioning purely as a storage/layout concern.

In Spark, partitioning directly affects execution.

The chain is:

```text
Partitioning
    ↓
Data distribution
    ↓
Task workload
    ↓
Executor utilization
    ↓
Stage completion time
    ↓
Application latency
```

Therefore partitioning decisions are performance decisions.

A partitioning scheme should be evaluated not only by:

```text
number of partitions
```

but also by:

```text
distribution of work
```

---

# 22. What This Experiment Proves

This experiment provides direct evidence for the following statements.

### Finding 1 — Hash partitioning introduces a shuffle

The physical plan contains:

```text
Exchange
hashpartitioning(distribution_key, 8)
```

Therefore:

```scala
repartition(8, $"distribution_key")
```

requires data redistribution.

---

### Finding 2 — Eight partitions do not imply balanced partitions

The output contained:

```text
10,526 → 705,263 rows
```

across the eight partitions.

---

### Finding 3 — Skewed keys can create severe partition imbalance

The input contained:

```text
500,000 occurrences of key 0
```

and much smaller long-tail key frequencies.

The resulting hash distribution contained:

```text
705,263 rows
```

in one partition.

---

### Finding 4 — Partition imbalance can produce uneven task execution

The heaviest partition was approximately:

```text
705K rows
```

and its task was among the slowest observed tasks.

---

### Finding 5 — Increasing partition count alone is not a complete skew strategy

A hot key remains a hot key.

The partitioning strategy must account for the key distribution itself.

---

# 23. Production Engineering Lesson

The most important lesson from this experiment is:

> Partition count is not the same thing as parallelism.

True effective parallelism depends on how much work each partition contains.

A job with:

```text
100 partitions
```

can still behave like a poorly parallelized job if:

```text
1 partition contains most of the data
99 partitions contain very little
```

The Spark UI may reveal this immediately through task-level input and duration distributions.

Therefore, when investigating a slow Spark stage, do not stop at:

```text
How many partitions are there?
```

Ask:

```text
How is the data distributed across those partitions?
```

That question often leads directly to the root cause.

---

# 24. Interview-Level Takeaways

### Q: Does `repartition(n, key)` guarantee equal-sized partitions?

No.

It requests an `n`-way hash partitioning scheme. Uneven key frequencies can result in significantly different partition sizes.

---

### Q: Why does data skew cause slow Spark jobs?

A small number of partitions can receive disproportionately large amounts of data. Their tasks then take longer, becoming stragglers that delay stage completion.

---

### Q: What is the difference between partition imbalance and data skew?

Data skew is an uneven distribution of values, usually keys.

Partition imbalance is the physical consequence: partitions contain significantly different amounts of data/work.

---

### Q: Can increasing `spark.sql.shuffle.partitions` fix skew?

Not necessarily.

Increasing partition count can help when partitions are uniformly too large, but it does not inherently solve a hot-key problem.

---

### Q: Does hash partitioning guarantee even distribution?

No.

It determines partition placement according to the key and partitioning scheme. It does not guarantee equal partition sizes.

---

### Q: Does AQE automatically eliminate all skew?

No.

AQE provides adaptive optimizations and specific skew-handling capabilities, but it is not a universal solution for arbitrary skew.

---

### Q: Why is one task still running when almost all other tasks have finished?

One possible reason is data skew.

The remaining task may be processing a disproportionately large partition.

---

# 25. Final Experiment Summary

```text
Input
1,000,000 rows
2 partitions
        │
        ▼
Deliberately skewed keys
        │
        ├── key 0 → 500K
        ├── key 1 → 200K
        ├── key 2 → 100K
        ├── key 3 → 50K
        ├── key 4 → 50K
        └── keys 5–99 → ~100K
        │
        ▼
repartition(8, distribution_key)
        │
        ▼
Exchange
hashpartitioning(distribution_key, 8)
        │
        ▼
8 output partitions
        │
        ├── P0 → 113,686
        ├── P1 → 17,893
        ├── P2 → 10,526
        ├── P3 → 62,631
        ├── P4 → 63,686
        ├── P5 → 705,263  ← hot partition
        ├── P6 → 14,736
        └── P7 → 11,579
```

The resulting:

```text
Maximum partition = 705,263 rows
Minimum partition = 10,526 rows
Max / Min         ≈ 67x
Largest partition = 70.5% of dataset
Ideal average     = 125,000 rows
```

provides strong empirical evidence that:

> A skewed key distribution can turn an apparently parallel eight-partition Spark workload into a highly imbalanced workload where one partition dominates execution.

---

# 26. Module 1.5.7 Conclusion

Partitioning is not merely about choosing a number such as:

```text
8
100
200
1000
```

The more important question is:

> How is the workload distributed across those partitions?

This experiment demonstrated the full chain:

```text
Skewed key distribution
        ↓
Hash partitioning
        ↓
Uneven hash buckets
        ↓
Partition imbalance
        ↓
Uneven task workloads
        ↓
Potential straggler
        ↓
Stage performance impact
```

That is the difference between knowing Spark's partitioning APIs and understanding how Spark actually executes distributed workloads.

**Module 1.5.7 establishes the foundation for the next stage of the investigation: understanding how Spark handles skew and what techniques can mitigate hot partitions.**
