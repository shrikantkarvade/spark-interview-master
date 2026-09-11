# Module 1.7.11 — Aggregation Strategy Comparison

> **Status:** Completed
> **Experiment Range:** Q0–Q20
> **Spark Version:** 3.5.1
> **Scala:** Scala
> **Execution Mode:** Local Spark
> **Primary Dataset:** 10,000,000 transactions
> **High-Cardinality Dimension:** 1,000,000 customers
> **Configured Shuffle Partitions:** 64
> **Primary Focus:** HashAggregate behavior, aggregation cardinality, aggregate state width, shuffle cost, AQE, and production optimization

---

## 1. Module Objective

This module experimentally compares Spark SQL aggregation behavior across different:

* aggregation functions
* group cardinalities
* aggregate-state widths
* distinct operations
* combined aggregations
* shuffle patterns
* AQE partition coalescing behavior

The objective is not simply to determine which SQL expression is "fastest."

The production objective is to understand:

> **How does Spark's physical aggregation strategy change as the number of groups and the amount of state maintained per group increase?**

The experiments use Spark UI metrics and physical plans as evidence.

---

# 2. Key Engineering Questions

This module answers the following questions:

1. What happens when aggregation cardinality increases?
2. How does HashAggregate memory change with cardinality?
3. How does aggregate state width affect shuffle volume?
4. Why does `AVG()` require more state than `COUNT()`?
5. What is the cost of `MIN()` + `MAX()`?
6. What happens with `COUNT(DISTINCT ...)`?
7. Why can a distinct aggregation require multiple shuffle boundaries?
8. Does `LIMIT 5` reduce aggregation cost?
9. Does an Exchange automatically mean the query has a shuffle bottleneck?
10. How does AQE react to small versus larger post-shuffle datasets?
11. When is combining several aggregations into one aggregation beneficial?
12. What metrics should be inspected when diagnosing aggregation performance?

---

# 3. Benchmark Configuration

The primary benchmark configuration was:

```text
Input rows:              10,000,000
Customers:                1,000,000
Initial shuffle partitions: 64
Source partitions:        2
Cached input size:       ~381.5 MiB
```

The generated dataset contains:

```text
transaction_id
customer_id
product_id
amount
transaction_ts
```

The important generated expressions are:

```scala
customer_id = pmod(id, 1000000)

product_id = pmod(id * 31, 100000)

amount = pmod(id * 17, 100000) / 100.0
```

Therefore:

* `customer_id` has up to **1,000,000 distinct values**
* `product_id` has up to **100,000 distinct values**
* the dataset is intentionally deterministic
* the workload is suitable for controlled aggregation experiments

---

# 4. Why HashAggregate Matters

Spark SQL commonly implements grouping and aggregation using `HashAggregate`.

Conceptually:

```text
Input rows
    |
    v
HashAggregate
    |
    +-- group key
    +-- aggregate state
    |
    v
Shuffle
    |
    v
Final HashAggregate
```

For example:

```sql
SELECT
    customer_id,
    COUNT(*),
    SUM(amount)
FROM transactions
GROUP BY customer_id;
```

Spark can perform partial aggregation before the shuffle:

```text
10M input rows
      |
      v
Partial HashAggregate
      |
      v
2M partial records
      |
      v
Exchange
      |
      v
Final HashAggregate
      |
      v
1M customer results
```

The important performance dimensions are:

```text
Number of groups
        +
Aggregate state per group
        +
Shuffle volume
        +
Partition distribution
```

---

# 5. Two Fundamental Variables

## 5.1 Cardinality

Cardinality is the number of distinct grouping keys.

Examples:

```text
10 groups
10,000 groups
1,000,000 groups
```

Higher cardinality generally means:

* more hash-map entries
* more memory
* more CPU
* more intermediate records
* potentially more shuffle data
* increased risk of spilling at larger production scale

---

## 5.2 Aggregate State Width

Each group must maintain some state.

For example:

### COUNT

```text
customer_id
count
```

### SUM

```text
customer_id
sum
```

### AVG

Conceptually:

```text
customer_id
sum
count
```

### MIN + MAX

```text
customer_id
min
max
```

Therefore:

> **Cardinality determines how many states Spark maintains. State width determines how much information each state carries.**

This distinction becomes one of the most important findings of this module.

---

# 6. Complete Experiment Matrix

The following table summarizes the completed evidence set.

| Query | Workload                                 | Max WSCG | Max HashAggregate | Peak Memory |    Shuffle | AQE  |
| ----- | ---------------------------------------- | -------: | ----------------: | ----------: | ---------: | ---- |
| Q0    | Simple `COUNT(*)`                        |     7.6s |              41ms |           — |      118 B | —    |
| Q1    | High-cardinality customer aggregation    |     1.5s |              1.2s |      96 MiB |    9.8 MiB | 64→2 |
| Q2    | Customer COUNT + LIMIT 5                 |     1.2s |             937ms |      96 MiB |   10.4 MiB | 64→2 |
| Q3    | Customer aggregation variant             |     1.1s |             934ms |      96 MiB |    9.8 MiB | 64→2 |
| Q4    | Customer SUM + LIMIT 5                   |     1.2s |             969ms |      96 MiB |   21.8 MiB | 64→2 |
| Q5    | Distinct customer count                  |     1.0s |             870ms |      96 MiB |    9.8 MiB | 64→2 |
| Q6    | Customer AVG + LIMIT 5                   |     1.3s |              1.0s |      96 MiB |   23.8 MiB | 64→2 |
| Q7    | Distinct customer count repeat           |    989ms |             825ms |      96 MiB |    9.8 MiB | 64→2 |
| Q8    | Customer MIN + MAX                       |     1.3s |              1.0s |      96 MiB |   31.1 MiB | 64→2 |
| Q9    | Distinct customer count repeat           |    962ms |             795ms |      96 MiB |    9.8 MiB | 64→2 |
| Q11   | Distinct customer count repeat           |    992ms |             847ms |      96 MiB |    9.8 MiB | 64→2 |
| Q13   | Distinct customer count                  |    965ms |             796ms |      96 MiB |    9.8 MiB | 64→2 |
| Q14   | COUNT + SUM + DISTINCT products          |     1.5s |              1.2s |      96 MiB | ~45.0 MiB* | 64→2 |
| Q15   | Low-cardinality key-only GROUP BY        |    155ms |             149ms |     256 KiB |      964 B | 64→1 |
| Q16   | Low-cardinality COUNT + SUM              |    211ms |             204ms |     256 KiB |    1,162 B | 64→1 |
| Q17   | Medium-cardinality key-only GROUP BY     |    170ms |             143ms |     256 KiB |  132.8 KiB | 64→1 |
| Q18   | Medium-cardinality COUNT + SUM           |    212ms |             187ms |     256 KiB |  262.5 KiB | 64→1 |
| Q19   | High-cardinality distinct customer count |     1.0s |             842ms |      96 MiB |    9.8 MiB | 64→2 |
| Q20   | High-cardinality COUNT + SUM             |     1.3s |              1.1s |      96 MiB |   22.2 MiB | 64→2 |

* Q14 actual Exchange shuffle traffic:

```text
First Exchange:  32.0 MiB
Second Exchange: 13.0 MiB
Total:           ~45.0 MiB
```

> **Important:** The table uses actual Spark UI `shuffle bytes written` for shuffle comparisons, not logical-plan `sizeInBytes` statistics.

---

# 7. Q0 — Simple COUNT Baseline

Q0 establishes the baseline.

The physical plan was approximately:

```text
InMemoryTableScan
    |
    v
Partial HashAggregate
    |
    v
SinglePartition Exchange
    |
    v
Final HashAggregate
```

Key evidence:

```text
Max WholeStageCodegen: 7.6s
Stage: 0.0
Task: 1

Max HashAggregate build: 41ms
Stage: 1.0
Task: 2

Shuffle: 118 B
```

The important lesson is:

> The 7.6-second WholeStageCodegen maximum must not be attributed entirely to HashAggregate.

The actual aggregation build was only:

```text
41 ms
```

This establishes an important UI-analysis rule:

> **WholeStageCodegen duration and HashAggregate duration are different metrics and must be interpreted separately.**

---

# 8. High-Cardinality Aggregation — Q1 to Q13

The high-cardinality experiments consistently produced approximately:

```text
2M intermediate aggregation records
~9.8 MiB shuffle
96 MiB peak aggregation memory
~0.8–1.0s maximum HashAggregate build
64 → 2 AQE coalescing
0 B spill
0 sort fallback
```

This became our high-cardinality baseline.

---

# 9. Q4 — SUM State Is Wider Than COUNT State

Q4 used:

```sql
GROUP BY customer_id
SUM(amount)
```

Evidence:

```text
2M aggregation output rows
21.8 MiB shuffle
96 MiB peak memory
~969 ms max HashAggregate build
64 → 2 AQE
```

Compared with COUNT-based aggregation, SUM must carry numeric aggregate state for each group.

The important lesson:

> **Adding aggregate state can increase shuffle payload even when group cardinality remains unchanged.**

---

# 10. Q6 — AVG Requires More State

Q6 used:

```sql
GROUP BY customer_id
AVG(amount)
```

The physical plan showed AVG as partial sum/count state.

Conceptually:

```text
partial_sum(amount)
partial_count(amount)
```

followed by:

```text
sum / count
```

Evidence:

```text
Shuffle: 23.8 MiB
Peak memory: 96 MiB
Max HashAggregate: ~1.0s
AQE: 64 → 2
```

Compare:

```text
SUM:  21.8 MiB
AVG:  23.8 MiB
```

This demonstrates:

> **AVG requires more aggregation state than SUM because the partial aggregation must preserve both sum and count.**

---

# 11. Q8 — MIN + MAX Widens the State Further

Q8 calculated:

```text
MIN(amount)
MAX(amount)
```

for each customer.

Evidence:

```text
Shuffle: 31.1 MiB
Peak memory: 96 MiB
Max HashAggregate: ~1.0s
AQE: 64 → 2
```

The state effectively contains:

```text
customer_id
min
max
```

Therefore:

```text
COUNT
   <
SUM
   <
AVG
   <
MIN + MAX
```

in observed shuffle footprint for these specific high-cardinality workloads.

This is not a claim that these functions have a universal runtime ranking. The result demonstrates the effect of **state width under this benchmark**.

---

# 12. Q10 — Combined Aggregation

Q10 combined:

```text
COUNT
SUM
AVG
MIN
MAX
```

in one aggregation.

The physical plan showed:

```text
partial_count
partial_sum
partial_avg
partial_min
partial_max
```

The Exchange carried the complete partial aggregation state.

The plan statistics reported approximately:

```text
122.1 MiB
2M rows
```

The important production lesson is:

> If an application genuinely needs multiple metrics, combining compatible aggregations can avoid repeated scans and repeated aggregation pipelines.

However:

> Combining metrics increases aggregate state width.

Therefore the production decision is not simply:

> "Use fewer aggregate functions."

It is:

> **Calculate the metrics actually required, and avoid redundant aggregation pipelines.**

---

# 13. Q14 — COUNT(DISTINCT product_id)

Q14 is one of the most structurally important experiments.

The query combined:

```text
COUNT(*)
SUM(amount)
COUNT(DISTINCT product_id)
```

grouped by:

```text
customer_id
```

The physical plan became:

```text
InMemoryTableScan
      |
      v
HashAggregate(customer_id, product_id)
      |
      v
Exchange(customer_id, product_id)
      |
      v
HashAggregate(customer_id, product_id)
      |
      v
HashAggregate(customer_id)
      |
      v
Exchange(customer_id)
      |
      v
Final HashAggregate
      |
      v
CollectLimit
```

This introduces **two Exchange boundaries**.

Actual shuffle traffic:

```text
First Exchange:  32.0 MiB
Second Exchange: 13.0 MiB
Total:           ~45.0 MiB
```

The workload produced:

```text
2M intermediate (customer, product) groups
1M customer-level groups
```

Key UI evidence:

```text
Max WSCG: 1.5s
Stage: 67
Task: 60

Max HashAggregate build: 1.2s
Stage: 67
Task: 61

Peak memory: 96 MiB
Stage: 67
Task: 60

Max shuffle write: 80ms
Stage: 67
Task: 61

Max first-shuffle bytes: 16.0 MiB
Stage: 67
Task: 60
```

Important distinction:

> The maximum WSCG task and maximum HashAggregate task were different tasks.

This is another example of why Spark UI metrics must be mapped individually.

---

# 14. COUNT(DISTINCT) Production Lesson

`COUNT(DISTINCT ...)` can fundamentally change the physical aggregation structure.

It may require:

* additional grouping
* additional aggregation stages
* additional shuffle boundaries
* more intermediate state

Therefore:

> **COUNT(DISTINCT) should be treated as a potentially expensive physical operation, not merely as a small variation of COUNT().**

At scale, approximate distinct-count techniques may sometimes be appropriate when exact cardinality is not required.

---

# 15. Q15 — Low Cardinality

Q15 grouped by:

```text
pmod(customer_id, 10)
```

Therefore the workload had only:

```text
10 groups
```

Evidence:

```text
Max WSCG: 155ms
Stage: 73
Task: 65

Max HashAggregate: 149ms
Stage: 73
Task: 65

Peak memory: 256 KiB
Stage: 73
Task: 66

Max shuffle write: 6ms
Stage: 73
Task: 66

Max shuffle bytes: 482 B
Stage: 73
Task: 66

Total shuffle: 964 B

AQE: 64 → 1
```

The key observation:

> The query still contains an Exchange, but the actual shuffle volume is negligible.

Therefore:

> **An Exchange is not automatically a performance problem.**

Always inspect actual metrics.

---

# 16. Q16 — Low Cardinality + Wider State

Q16 used:

```text
COUNT(*)
SUM(amount)
```

with the same 10-group cardinality.

Evidence:

```text
Max WSCG: 211ms
Stage: 79
Task: 70

Max HashAggregate: 204ms
Stage: 79
Task: 70

Peak memory: 256 KiB
Stage: 79
Task: 69

Max shuffle write: 7ms
Stage: 79
Task: 69

Max shuffle bytes: 581 B
Stage: 79
Task: 69

Total shuffle: 1,162 B

AQE: 64 → 1
```

Compared with Q15:

```text
Q15: 964 B
Q16: 1,162 B
```

Same cardinality.

More aggregate state.

More shuffle bytes.

This is a clean state-width experiment.

---

# 17. Q17 — Medium Cardinality

Q17 grouped by:

```text
pmod(customer_id, 10000)
```

giving:

```text
10,000 groups
```

Evidence:

```text
Max WSCG: 170ms
Stage: 82
Task: 72

Max HashAggregate: 143ms
Stage: 82
Task: 72

Peak memory: 256 KiB
Stage: 82
Task: 72

Max shuffle write: 30ms
Stage: 82
Task: 73

Max shuffle bytes: 66.4 KiB
Stage: 82
Task: 72

Total shuffle: 132.8 KiB

Shuffle records: 20,000

AQE: 64 → 1
```

The cardinality increased:

```text
10 → 10,000
```

yet the workload remained small enough that peak memory remained:

```text
256 KiB
```

This shows that aggregation cost does not necessarily scale linearly with cardinality in small local experiments.

Fixed Spark/JVM/task overhead can dominate short runs.

---

# 18. Q18 — Medium Cardinality + Wider State

Q18 kept the same:

```text
10,000 groups
```

but added:

```text
COUNT(*)
SUM(amount)
```

Evidence:

```text
Max WSCG: 212ms
Stage: 88
Task: 76

Max HashAggregate: 187ms
Stage: 88
Task: 76

Post-shuffle HashAggregate: 11ms
Stage: 88
Task: 77

Peak memory: 256 KiB
Stage: 88
Task: 77

Max shuffle write: 27ms
Stage: 88
Task: 77

Max shuffle bytes: 131.3 KiB
Stage: 88
Task: 77

Total shuffle: 262.5 KiB

AQE: 64 → 1
```

Compare Q17:

```text
Q17: 132.8 KiB
Q18: 262.5 KiB
```

The number of shuffle records remained:

```text
20,000
```

but the shuffle footprint nearly doubled.

This is one of the strongest controlled demonstrations in the module:

> **At fixed cardinality, wider aggregate state increases shuffle volume even when the number of records remains unchanged.**

---

# 19. Q19 — High Cardinality DISTINCT

Q19 returned to the full:

```text
customer_id
```

cardinality.

Distinct customer count:

```text
1,000,000
```

Physical structure:

```text
InMemoryTableScan
      |
      v
HashAggregate(customer_id)
      |
      v
Exchange(customer_id, 64)
      |
      v
AQEShuffleRead
      |
      v
HashAggregate(customer_id)
      |
      v
partial_count
      |
      v
SinglePartition Exchange
      |
      v
Final count
```

Exact UI evidence:

```text
Max WSCG:
~1.0s
Stage 91
Task 79

Max main HashAggregate:
842ms
Stage 91
Task 80

Max post-shuffle HashAggregate:
325ms
Stage 93
Task 81

Peak memory:
96 MiB
Stage 91
Task 80

Max shuffle write:
39ms
Stage 91
Task 80

Max shuffle bytes:
4.9 MiB
Stage 91
Task 80

Total shuffle:
9.8 MiB

Shuffle records:
2,000,000

Average hash probes:
1.4

Spill:
0 B

Sort fallback:
0

AQE:
64 → 2
```

This is the high-cardinality baseline.

---

# 20. Q20 — High Cardinality COUNT + SUM

Q20 used:

```text
GROUP BY customer_id

COUNT(*)
SUM(amount)
```

with:

```text
1,000,000 customers
```

The physical plan was:

```text
InMemoryTableScan
      |
      v
HashAggregate
  partial_count
  partial_sum
      |
      v
Exchange
  hashpartitioning(customer_id, 64)
      |
      v
AQEShuffleRead
  coalesced
      |
      v
HashAggregate
  count
  sum
      |
      v
CollectLimit 5
```

Exact evidence:

```text
Max WSCG:
1.3s
Stage 97
Task 84

Max HashAggregate build:
1.1s
Stage 97
Task 84

Peak memory:
96 MiB
Stage 97
Task 85

Max shuffle write:
53ms
Stage 97
Task 85

Max shuffle bytes:
11.1 MiB
Stage 97
Task 85

Total shuffle:
22.2 MiB

Shuffle records:
2,000,000

Post-shuffle HashAggregate:
322ms

Average hash probes:
1.4

Spill:
0 B

Sort fallback:
0

AQE:
64 → 2
```

Again:

> Task 84 is the aggregation-build maximum, while Task 85 is the memory/shuffle maximum.

---

# 21. Q19 vs Q20 — State Width at High Cardinality

This is an important comparison.

| Metric            |                     Q19 |          Q20 |
| ----------------- | ----------------------: | -----------: |
| Cardinality       |                      1M |           1M |
| Aggregation       | DISTINCT customer count |  COUNT + SUM |
| Max HashAggregate |                   842ms |         1.1s |
| Peak memory       |                  96 MiB |       96 MiB |
| Shuffle records   |                      2M |           2M |
| Shuffle           |                 9.8 MiB | **22.2 MiB** |
| AQE               |                    64→2 |         64→2 |
| Spill             |                       0 |            0 |

The number of records is identical:

```text
2,000,000
```

but Q20 carries more aggregation state through the shuffle:

```text
customer_id
count
sum
```

Therefore:

> **At high cardinality, wider state produces substantially more shuffle traffic.**

---

# 22. Q15 → Q17 → Q19 — Cardinality Ladder

This is the strongest controlled cardinality sequence.

| Query | Groups | Max HashAgg | Peak Memory |     Shuffle |      AQE |
| ----- | -----: | ----------: | ----------: | ----------: | -------: |
| Q15   |     10 |       149ms |     256 KiB |       964 B |     64→1 |
| Q17   |    10K |       143ms |     256 KiB |   132.8 KiB |     64→1 |
| Q19   |     1M |   **842ms** |  **96 MiB** | **9.8 MiB** | **64→2** |

The key transition is:

```text
10K groups
     ↓
1M groups
```

At this point:

```text
HashAggregate CPU increases substantially
Memory increases dramatically
Shuffle moves from KiB to MiB
AQE retains 2 partitions instead of 1
```

This is the clearest demonstration of **cardinality-driven aggregation cost**.

---

# 23. Q17 → Q18 — State Width Ladder

The clean controlled state-width comparison is:

| Metric          |       Q17 |           Q18 |
| --------------- | --------: | ------------: |
| Groups          |       10K |           10K |
| Aggregation     |  Key-only |   COUNT + SUM |
| Shuffle records |       20K |           20K |
| Shuffle         | 132.8 KiB | **262.5 KiB** |
| Peak memory     |   256 KiB |       256 KiB |
| AQE             |      64→1 |          64→1 |

The number of groups and records is fixed.

Only the aggregation state changes.

Result:

```text
132.8 KiB
     ↓
262.5 KiB
```

This is strong evidence that:

> **Aggregate state width directly affects the amount of data crossing the shuffle boundary.**

---

# 24. AQE Findings

AQE was particularly visible in the cardinality experiments.

### Low cardinality

```text
Configured: 64
Actual:      1
```

### Medium cardinality

```text
Configured: 64
Actual:      1
```

### High cardinality

```text
Configured: 64
Actual:      2
```

This demonstrates:

> `spark.sql.shuffle.partitions = 64` describes the configured shuffle partitioning, not necessarily the final number of physical partitions processed after AQE.

AQE can coalesce small shuffle partitions based on actual runtime data.

---

# 25. Why AQE Matters for Aggregations

Without AQE, a tiny aggregation could unnecessarily process many tiny partitions.

For example:

```text
10 groups
64 configured partitions
```

would be an inefficient physical layout.

AQE recognized that the data was tiny and reduced the number of coalesced partitions:

```text
64 → 1
```

For the high-cardinality workload:

```text
1M groups
9.8–22.2 MiB shuffle
```

AQE retained:

```text
2 partitions
```

Therefore:

> **AQE can reduce unnecessary post-shuffle task overhead without requiring manual partition-count tuning for every workload size.**

---

# 26. Exchange Does Not Mean Bottleneck

Q15 is the clearest example.

It had:

```text
Exchange
```

but only:

```text
964 B shuffle
6 ms max shuffle write
```

Therefore it would be incorrect to say:

> "The Exchange is causing the performance problem."

The correct analysis is:

```text
Exchange exists
        ↓
Actual data volume is tiny
        ↓
Shuffle is not the bottleneck
```

Production diagnosis should use actual metrics.

---

# 27. LIMIT Does Not Reduce Upstream Aggregation

Several queries included:

```sql
LIMIT 5
```

The physical plan consistently placed:

```text
CollectLimit
```

after the aggregation.

For Q20:

```text
HashAggregate
      ↓
CollectLimit 5
```

Therefore:

```text
10M rows
    ↓
1M customer groups
    ↓
shuffle
    ↓
final aggregation
    ↓
LIMIT 5
```

The `LIMIT` only reduces the number of rows returned/collected.

It does not avoid the upstream aggregation.

Production lesson:

> **Do not assume that `LIMIT` makes an expensive GROUP BY cheap.**

---

# 28. Spill Analysis

The aggregation experiments consistently showed:

```text
Spill size: 0 B
Sort fallback tasks: 0
```

even for the high-cardinality workloads.

Therefore, within this benchmark:

> **The observed performance cost is not caused by disk spilling.**

Instead, the dominant factors are:

```text
HashAggregate CPU
Hash-map memory
Aggregation cardinality
Aggregate-state width
Shuffle volume
```

This distinction is critical for production troubleshooting.

Do not tune spill-related configuration simply because a HashAggregate is large.

First establish whether spilling actually occurs.

---

# 29. Hash Probe Behavior

High-cardinality workloads showed approximately:

```text
1.4 average hash probes per key
```

while very low-cardinality experiments showed negligible probe activity.

This is supporting evidence that larger aggregation state creates more substantial hash-table work.

However:

> Hash probes should be treated as a diagnostic signal rather than a standalone optimization target.

The more important production metrics remain:

* aggregation build time
* peak memory
* spill
* shuffle bytes
* shuffle write time
* partition distribution

---

# 30. WholeStageCodegen vs HashAggregate

One of the most important Spark UI lessons from this module is:

> **Do not equate WholeStageCodegen duration with HashAggregate duration.**

For example, Q20:

```text
WholeStageCodegen max:
1.3s
Stage 97 / Task 84

HashAggregate build max:
1.1s
Stage 97 / Task 84
```

The values are related because HashAggregate executes inside the generated stage, but WholeStageCodegen represents the broader generated execution region.

Similarly, Q19:

```text
WSCG max:
~1.0s
Stage 91 / Task 79

HashAggregate max:
842ms
Stage 91 / Task 80
```

Different tasks.

Therefore performance analysis should report them separately.

---

# 31. Max Task Mapping Rules

For Spark UI analysis, always record:

```text
Metric
Maximum
Stage ID
Task ID
```

Examples from the final experiments:

### Q19

```text
WSCG:
~1.0s
Stage 91 / Task 79

HashAggregate:
842ms
Stage 91 / Task 80

Peak memory:
96 MiB
Stage 91 / Task 80
```

### Q20

```text
WSCG:
1.3s
Stage 97 / Task 84

HashAggregate:
1.1s
Stage 97 / Task 84

Peak memory:
96 MiB
Stage 97 / Task 85

Shuffle:
11.1 MiB
Stage 97 / Task 85
```

This distinction is essential when diagnosing skew or task-level bottlenecks.

---

# 32. Production Aggregation Optimization Framework

Based on Q0–Q20, the recommended production workflow is:

## Step 1 — Determine cardinality

Ask:

```text
How many groups will this aggregation create?
```

Estimate:

```text
10
10K
1M
100M
```

High cardinality should immediately trigger deeper investigation.

---

## Step 2 — Determine aggregate state

Identify:

```text
COUNT
SUM
AVG
MIN
MAX
DISTINCT
```

and determine how much state each group must carry.

---

## Step 3 — Inspect the physical plan

Look for:

```text
HashAggregate
Exchange
AQEShuffleRead
SortAggregate
CollectLimit
```

Determine how many shuffle boundaries exist.

---

## Step 4 — Inspect Spark UI

Capture:

```text
HashAggregate build time
Peak memory
Spill
Sort fallback
Shuffle bytes
Shuffle records
Shuffle write time
Hash probes
```

---

## Step 5 — Inspect AQE

Determine:

```text
Configured partitions
Actual coalesced partitions
Partition data sizes
```

Do not infer the physical task count from the logical Exchange alone.

---

## Step 6 — Look for unnecessary cardinality

Ask:

```text
Can the grouping key be reduced?
Can an unnecessary dimension be removed?
Can aggregation happen earlier?
```

Reducing:

```text
1M groups → 100K groups
```

can have a major impact.

---

## Step 7 — Reduce unnecessary state

Only calculate the metrics actually required.

Avoid carrying unused:

```text
SUM
AVG
MIN
MAX
```

through a large shuffle.

---

## Step 8 — Combine required aggregations

If the business requires:

```text
COUNT
SUM
AVG
MIN
MAX
```

one well-designed aggregation can often be preferable to repeatedly scanning the same data.

---

## Step 9 — Treat DISTINCT separately

Investigate:

```text
COUNT(DISTINCT ...)
```

explicitly.

Check whether it introduces:

```text
additional grouping
additional Exchange
additional aggregation
```

---

## Step 10 — Investigate spills only when they exist

If:

```text
spill = 0
```

do not make spill tuning the first optimization.

---

# 33. Common Mistakes This Module Prevents

## Mistake 1

> "There is an Exchange, so the query is slow."

Incorrect.

Q15 proves that an Exchange can process less than 1 KB.

---

## Mistake 2

> "LIMIT 5 means Spark only processes five groups."

Incorrect.

The aggregation occurs before `CollectLimit`.

---

## Mistake 3

> "WholeStageCodegen took 1.3 seconds, so HashAggregate took 1.3 seconds."

Incorrect.

WSCG and operator metrics must be interpreted separately.

---

## Mistake 4

> "96 MiB memory means Spark is spilling."

Incorrect.

Q19/Q20 showed:

```text
96 MiB
spill = 0 B
```

---

## Mistake 5

> "64 shuffle partitions means 64 post-shuffle tasks."

Incorrect when AQE is enabled.

The experiments repeatedly showed:

```text
64 → 1
```

or:

```text
64 → 2
```

---

## Mistake 6

> "AVG is just SUM divided by COUNT with no extra aggregation state."

Incorrect.

The partial aggregation must preserve both:

```text
sum
count
```

---

## Mistake 7

> "COUNT(DISTINCT) is just COUNT."

Incorrect.

Q14 demonstrates that distinct processing can substantially change the physical execution plan.

---

# 34. Interview-Level Questions and Answers

## Q: What determines Spark aggregation memory usage?

**Answer:**

Primarily the number of groups and the amount of aggregation state maintained for each group.

A useful mental model is:

```text
Aggregation memory
≈
number of groups
×
state maintained per group
```

Actual memory usage also depends on Spark's internal data structures and execution details.

---

## Q: Why can SUM generate more shuffle data than COUNT?

Because each group must carry a numeric sum state in addition to the grouping key.

---

## Q: Why does AVG require more state?

Because Spark needs both:

```text
sum
count
```

to compute the final average.

---

## Q: Why is COUNT(DISTINCT) expensive?

Because Spark must identify unique values, which can require additional grouping, aggregation, and shuffle processing.

---

## Q: Does LIMIT reduce GROUP BY cost?

Not when it appears after the aggregation.

---

## Q: Does an Exchange always indicate a performance bottleneck?

No.

The actual shuffle volume and execution metrics must be inspected.

---

## Q: What does AQE do for these aggregations?

AQE dynamically adapts the post-shuffle execution, including coalescing small shuffle partitions.

---

## Q: What should you inspect first when HashAggregate is slow?

Inspect:

1. group cardinality
2. aggregate state width
3. peak memory
4. spill
5. shuffle volume
6. partition distribution
7. AQE behavior

---

# 35. Final Experimental Findings

The complete experiment set establishes the following:

### Finding 1 — Cardinality matters

```text
10 groups
→
10K groups
→
1M groups
```

eventually produces a substantial change in aggregation memory and CPU.

---

### Finding 2 — State width matters

At the same cardinality:

```text
key-only
```

requires less shuffle state than:

```text
COUNT + SUM
```

---

### Finding 3 — AVG carries additional state

AVG requires sum and count state.

---

### Finding 4 — Multiple aggregates widen the shuffle

MIN + MAX and combined aggregations carry more information per group.

---

### Finding 5 — DISTINCT can change the physical strategy

Q14 introduced an additional aggregation/shuffle structure.

---

### Finding 6 — AQE adapts to actual data

```text
64 → 1
```

for small workloads and:

```text
64 → 2
```

for larger high-cardinality workloads.

---

### Finding 7 — No spill occurred

The high-cardinality workloads were memory-intensive but remained spill-free under this benchmark.

---

### Finding 8 — LIMIT is not an upstream optimization

`CollectLimit` occurs after aggregation.

---

### Finding 9 — Exchange must be evaluated quantitatively

An Exchange with 964 B of data is not equivalent to an Exchange moving hundreds of gigabytes.

---

### Finding 10 — Production optimization starts with cardinality

Before tuning Spark configuration, ask:

> **How many groups am I creating, and do I really need all of them?**

---

# 36. Final Mental Model

The most useful mental model from Module 1.7.11 is:

```text
                    SPARK AGGREGATION COST
                              |
                 +------------+------------+
                 |                         |
                 v                         v
             CARDINALITY              STATE WIDTH
                 |                         |
                 |                         |
          Number of groups          Bytes per group
                 |                         |
                 v                         v
           Hash-map size             Shuffle payload
           CPU / memory              Network / memory
                 |                         |
                 +------------+------------+
                              |
                              v
                         SHUFFLE
                              |
                              v
                            AQE
                              |
                              v
                    Physical execution
```

The central production equation is therefore:

```text
Aggregation complexity
≈
cardinality
×
state width
+
shuffle cost
+
partition/distribution effects
```

This is a conceptual model rather than a literal Spark runtime formula.

---

# 37. Production Decision Checklist

Before deploying a large aggregation, ask:

```text
[ ] What is the expected group cardinality?

[ ] Is the grouping key necessary?

[ ] Can aggregation happen earlier?

[ ] What state does each aggregate require?

[ ] Are COUNT/SUM/AVG/MIN/MAX all actually required?

[ ] Is COUNT(DISTINCT) required?

[ ] How many Exchange operators exist?

[ ] How much data crosses each Exchange?

[ ] What does AQE do with the shuffle partitions?

[ ] Is HashAggregate spilling?

[ ] Is sort fallback occurring?

[ ] Which task has the maximum aggregation time?

[ ] Which task has maximum aggregation memory?

[ ] Are those the same task?

[ ] Is there evidence of skew?

[ ] Is LIMIT occurring before or after the expensive aggregation?
```

---

# 38. Module 1.7.11 Final Conclusion

Module 1.7.11 demonstrates that Spark aggregation optimization is not primarily about memorizing which aggregate function is fastest.

The key production questions are:

```text
How many groups?

How much state per group?

How much data crosses the shuffle?

How many shuffle boundaries exist?

How does AQE reshape the execution?

Is the aggregation spilling?

Is the workload skewed?
```

The strongest experimental evidence came from:

```text
Q15 → Q17 → Q19
```

for **cardinality**, and:

```text
Q17 → Q18
```

for **aggregate-state width**.

Q14 demonstrated the additional complexity introduced by:

```text
COUNT(DISTINCT ...)
```

while Q20 demonstrated the high-cardinality cost of carrying multiple aggregation states.

The most important production principle is:

> **When optimizing Spark aggregations, don't start by asking which aggregation function is fastest. Start by asking how many groups you are creating, how much state each group must carry, and how much data must cross the shuffle boundary.**

---

# 39. Evidence Summary

### Dataset

```text
10,000,000 rows
1,000,000 customers
381.5 MiB cached input
64 configured shuffle partitions
```

### High-cardinality baseline

```text
~1M groups
96 MiB peak HashAggregate memory
~0.8–1.1s max aggregation build
~9.8–22.2 MiB shuffle
64 → 2 AQE
0 spill
```

### Low-cardinality baseline

```text
10 groups
256 KiB peak memory
964 B – 1,162 B shuffle
64 → 1 AQE
0 spill
```

### Medium-cardinality baseline

```text
10K groups
256 KiB peak memory
132.8 – 262.5 KiB shuffle
64 → 1 AQE
0 spill
```

### Distinct-combination workload

```text
COUNT
+
SUM
+
COUNT(DISTINCT product_id)

~45 MiB actual Exchange traffic
Two shuffle boundaries
96 MiB peak memory
0 spill
64 → 2 AQE
```

---

# 40. Module Status

**Module 1.7.11 — Aggregation Strategy Comparison: COMPLETE**

Experimental evidence:

```text
Q0 → Q20
```

covered:

```text
✓ Low-cardinality aggregation
✓ Medium-cardinality aggregation
✓ High-cardinality aggregation
✓ COUNT
✓ SUM
✓ AVG
✓ MIN
✓ MAX
✓ COUNT(DISTINCT)
✓ Combined aggregations
✓ Aggregate-state width
✓ HashAggregate memory
✓ HashAggregate CPU
✓ Shuffle volume
✓ Shuffle records
✓ AQE coalescing
✓ LIMIT behavior
✓ Spill analysis
✓ Sort fallback analysis
✓ Physical-plan analysis
✓ Stage/Task metric mapping
✓ Production optimization framework
```

**Next module:** Module 1.7.12 — **Production Aggregation Optimization**

The `AggregationMemorySpillExperiment` should be retained as a **supporting experiment under 1.7.12**, rather than treated as part of the main 1.7.11 aggregation-strategy comparison.
