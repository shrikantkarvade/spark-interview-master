# Module 1.4.11 — Data Source Filter & Projection Pushdown

## Objective

Understand what happens after Catalyst optimization when Spark reads a Parquet data source:

* How predicates are pushed toward the data source.
* How unused columns are eliminated from the physical scan.
* How to distinguish Catalyst predicate optimization from data-source filter pushdown.
* How to read `PushedFilters` and `ReadSchema` in a production Spark physical plan.

---

## Core Question

> When a DataFrame reads Parquet, how does Spark push filters and projections toward the data source to reduce the amount of data that needs to be processed?

---

## Use Case

Consider a transaction dataset stored as Parquet:

```text
id
customer_id
customer_name
amount
```

A downstream application needs only:

```text
customer_id
amount
```

for:

```text
customer_id = 42
```

A naive execution model would be:

```text
Read entire Parquet file
        ↓
Read all four columns
        ↓
Filter customer_id = 42
        ↓
Discard unused columns
```

Spark can optimize this execution.

The query planner can push the supported predicate toward the Parquet data source and prune columns that are not required by the query.

Conceptually:

```text
Parquet
   │
   ├── Filter: customer_id = 42
   │
   └── Read only:
          customer_id
          amount
```

This reduces downstream processing and can reduce physical I/O depending on the data source, file layout, metadata, compression, and predicate selectivity.

---

# Experiment Setup

## Spark Environment

| Property    | Value      |
| ----------- | ---------- |
| Spark       | 3.5.1      |
| Java        | 17.0.20.1  |
| Scala       | 2.12.18    |
| OS          | Windows 11 |
| Master      | `local[2]` |
| Data format | Parquet    |

---

## Parquet Fixture

A standalone Parquet fixture was generated with 100,000 rows.

Schema:

```text
message spark_pushdown_fixture {
  required int64 id;
  required int64 customer_id;
  required binary customer_name (UTF8);
  required int64 amount;
}
```

The generated fixture contains four columns:

```text
id
customer_id
customer_name
amount
```

The fixture was generated independently of Spark's DataFrame writer because the Windows development environment does not have Hadoop `winutils.exe` configured.

Generated file:

```text
build/module-1.4.11/parquet/fixture.parquet
```

Observed file size:

```text
~1.7 MiB
```

Using a standalone Parquet writer isolates the read-side experiment from the Windows Hadoop output-commit issue encountered while generating the fixture through Spark.

---

# Query

The experiment reads the Parquet file, filters by customer, and selects only the required columns:

```scala
val result =
  spark.read
    .parquet(inputPath)
    .filter($"customer_id" === 42)
    .select(
      $"customer_id",
      $"amount"
    )
```

The logical intent is:

```text
Read Parquet
   ↓
customer_id = 42
   ↓
keep customer_id and amount
```

---

# Query Planning

## Parsed Logical Plan

Spark initially represents the query approximately as:

```text
Project [customer_id, amount]
+- Filter (customer_id = 42)
   +- Relation [id,customer_id,customer_name,amount] parquet
```

At this point the relation exposes all four columns.

---

## Analyzed Logical Plan

After Catalyst analysis, the attributes and data types are resolved:

```text
Project [customer_id#1L, amount#3L]
+- Filter (customer_id#1L = cast(42 as bigint))
   +- Relation [id#0L,customer_id#1L,customer_name#2,amount#3L] parquet
```

The column references are now resolved to Spark attributes with concrete types.

---

# Optimized Logical Plan

Catalyst produced:

```text
Project [customer_id#1L, amount#3L]
+- Filter (isnotnull(customer_id#1L) AND (customer_id#1L = 42))
   +- Relation [id#0L,customer_id#1L,customer_name#2,amount#3L] parquet
```

Two important observations can be made.

### 1. Catalyst incorporated nullability into the predicate

The optimized predicate became:

```text
isnotnull(customer_id)
AND
customer_id = 42
```

### 2. Only the required columns survive the projection requirements

The underlying relation contains:

```text
id
customer_id
customer_name
amount
```

but the query requires only:

```text
customer_id
amount
```

That requirement becomes visible in the physical scan.

---

# Physical Plan

The physical plan was:

```text
*(1) Filter (isnotnull(customer_id#1L) AND (customer_id#1L = 42))
+- *(1) ColumnarToRow
   +- FileScan parquet
```

The complete scan details were:

```text
FileScan parquet
  Output:
    customer_id
    amount

  Batched:
    true

  PushedFilters:
    [IsNotNull(customer_id), EqualTo(customer_id,42)]

  ReadSchema:
    struct<customer_id:bigint,amount:bigint>
```

There is no:

```text
Exchange
```

so this query does not introduce a shuffle boundary.

---

# Evidence 1 — Data Source Filter Pushdown

This is the most important evidence from the experiment.

Spark logged:

```text
FileSourceStrategy: Pushed Filters:
IsNotNull(customer_id),EqualTo(customer_id,42)
```

The physical plan independently confirms:

```text
PushedFilters:
[IsNotNull(customer_id), EqualTo(customer_id,42)]
```

This demonstrates **data-source filter pushdown**.

The predicate was not merely retained as a Spark-level filter above a full scan.

Spark communicated the supported predicate to the Parquet data source.

The runtime Parquet reader subsequently logged:

```text
Filtering using predicate:
and(noteq(customer_id, null), eq(customer_id, 42))
```

This gives direct evidence that the predicate reached the Parquet reader.

---

# Evidence 2 — Projection Pruning

The original Parquet schema contains four columns:

```text
id
customer_id
customer_name
amount
```

However, the physical scan reports:

```text
Output [2]: [customer_id#1L, amount#3L]
```

and:

```text
ReadSchema:
struct<customer_id:bigint,amount:bigint>
```

Therefore the physical scan requests only:

```text
customer_id
amount
```

The unused columns:

```text
id
customer_name
```

are not part of the scan's `ReadSchema`.

This is **projection pruning**.

The transformation can be summarized as:

```text
Logical relation:
4 columns

        ↓

Query requires:
2 columns

        ↓

Physical Parquet scan:
2 columns
```

---

# Evidence 3 — Columnar Scan

The physical scan reports:

```text
Batched: true
```

The plan then contains:

```text
ColumnarToRow
```

The execution path is therefore:

```text
Parquet
   ↓
Columnar scan
   ↓
ColumnarToRow
   ↓
Filter
```

The `ColumnarToRow` operator converts the columnar output into Spark's row representation for downstream processing.

---

# Runtime Evidence

The experiment successfully read:

```text
file:///C:/Users/shrik/IdeaProjects/spark-interview-master/
build/module-1.4.11/parquet/fixture.parquet
```

The actual `show(20)` execution created:

```text
ResultStage 1
```

with one task.

The task completed successfully:

```text
Finished task 0.0
ResultStage 1 finished
Job 1 finished
```

No shuffle stages were created.

The Parquet reader reported:

```text
FileScanRDD: Reading File path:
file:///.../fixture.parquet
```

and:

```text
FilterCompat: Filtering using predicate:
and(noteq(customer_id, null), eq(customer_id, 42))
```

The query returned:

```text
+-----------+------+
|customer_id|amount|
+-----------+------+
|42         |420   |
|42         |1420  |
|42         |2420  |
|42         |3420  |
|42         |4420  |
|42         |5420  |
|42         |6420  |
|42         |7420  |
|42         |8420  |
|42         |9420  |
|42         |10420 |
|42         |11420 |
|42         |12420 |
|42         |13420 |
|42         |14420 |
|42         |15420 |
|42         |16420 |
|42         |17420 |
|42         |18420 |
|42         |19420 |
+-----------+------+
```

The result validates the predicate:

```text
customer_id = 42
```

and the fixture's amount calculation:

```text
amount = id * 10
```

---

# Catalyst Predicate Optimization vs Data Source Filter Pushdown

These concepts are related, but they are not the same thing.

## Catalyst Predicate Optimization

Earlier Module 1.4 experiments demonstrated transformations such as:

```text
Filter
Filter
  ↓
single combined Filter
```

and:

```text
Project
Filter
  ↓
Filter
Project
```

These are transformations of Spark's logical plan and occur inside Catalyst.

---

## Data Source Filter Pushdown

This experiment demonstrates a later boundary:

```text
Spark physical plan
        ↓
FileScan parquet
        ↓
Parquet reader
```

The physical scan contains:

```text
PushedFilters:
[IsNotNull(customer_id), EqualTo(customer_id,42)]
```

The predicate has therefore been communicated to the data source.

A precise description is:

> Catalyst optimized the logical query, and Spark's data-source planning pushed the supported predicate into the Parquet scan.

This is more precise than simply saying:

> "Catalyst pushed the filter to Parquet."

---

# Projection Pruning vs Filter Pushdown

The experiment demonstrates two different optimizations.

## Filter Pushdown

Query predicate:

```text
customer_id = 42
```

Physical evidence:

```text
PushedFilters:
[IsNotNull(customer_id), EqualTo(customer_id,42)]
```

---

## Projection Pruning

Original schema:

```text
id
customer_id
customer_name
amount
```

Physical evidence:

```text
ReadSchema:
struct<customer_id:bigint,amount:bigint>
```

Together:

```text
              Parquet
                 │
        ┌────────┴────────┐
        │                 │
   Filter pushed      Columns pruned
 customer_id = 42     customer_id
                      amount
        │                 │
        └────────┬────────┘
                 ↓
          Spark execution
```

---

# What Spark Did Not Prove

It would be incorrect to claim:

> "Spark only reads the rows where customer_id = 42 from disk."

That is too strong.

A Parquet reader may still need to inspect file metadata and row-group metadata. The amount of physical I/O avoided depends on factors including:

* Parquet row-group boundaries
* column statistics
* predicate selectivity
* file layout
* compression
* metadata availability
* data-source capabilities

Therefore the defensible production statement is:

> Spark pushed the supported predicate to the Parquet reader and pruned unused columns from the scan, allowing the data source to avoid unnecessary processing and potentially reduce physical I/O.

---

# Production Engineering Interpretation

When reviewing a Spark job in production, do not stop at:

```text
Filter
Project
```

Inspect the scan itself.

For Parquet, important fields include:

```text
FileScan parquet
PushedFilters
ReadSchema
Batched
PartitionFilters
Location
```

A practical review checklist is:

```text
1. Is the predicate present in PushedFilters?
2. Are unnecessary columns absent from ReadSchema?
3. Is the scan columnar?
4. Is there an unexpected Exchange?
5. Is the filter being applied before an expensive operation?
6. Is the data source capable of accepting the predicate?
7. Are file statistics/layout likely to make the predicate selective?
```

This is more useful than simply stating:

> "Spark automatically optimizes the query."

The physical plan tells us **what Spark actually decided to execute**.

---

# Why This Matters at Scale

Consider a production table with:

```text
10 TB
100 columns
billions of rows
```

Suppose an application requires only:

```text
customer_id
amount
```

for:

```text
customer_id = 42
```

Without effective projection pruning and filter pushdown, the execution pipeline may perform unnecessary work reading and processing data that the application will ultimately discard.

With:

```text
PushedFilters
+
ReadSchema
```

Spark can reduce the amount of information entering the execution pipeline.

The impact becomes increasingly important when combined with:

* Parquet
* partitioned datasets
* selective predicates
* Parquet row-group statistics
* compression
* object-storage data lakes
* large numbers of files

---

# Physical Plan Reading Pattern

For production Spark debugging, use the following mental model:

```text
FileScan
   │
   ├── Location
   │
   ├── ReadSchema
   │
   ├── PushedFilters
   │
   ├── PartitionFilters
   │
   └── Batched
```

Interpret these fields separately.

### `ReadSchema`

What columns are being requested from the data source?

### `PushedFilters`

Which supported predicates are being delegated to the data source?

### `PartitionFilters`

Which predicates can eliminate dataset partitions/directories before file scanning?

### `Batched`

Is Spark using a columnar scan path?

### `Location`

Where is the data actually being read from?

---

# Important Distinction: File Filtering vs Partition Pruning

It is important not to confuse:

```text
PushedFilters
```

with:

```text
PartitionFilters
```

This experiment produced:

```text
PushedFilters:
[IsNotNull(customer_id), EqualTo(customer_id,42)]
```

but:

```text
PartitionFilters:
[]
```

That is expected because the fixture is a single Parquet file and `customer_id` is not a partition-directory column.

The concepts are different:

```text
Partition pruning
        ↓
Eliminate entire dataset partitions/directories

Filter pushdown
        ↓
Delegate supported predicates to the file/data source
```

A production table can benefit from both.

For example:

```text
WHERE year = 2026
  AND customer_id = 42
```

could conceptually result in:

```text
PartitionFilters:
year = 2026

PushedFilters:
customer_id = 42
```

This is an important distinction when diagnosing large data-lake scans.

---

# Experiment Result

| Observation                                  | Result                                            |
| -------------------------------------------- | ------------------------------------------------- |
| Parquet read                                 | Successful                                        |
| Filter                                       | `customer_id = 42`                                |
| Filter pushed to source                      | Yes                                               |
| `PushedFilters`                              | `IsNotNull(customer_id), EqualTo(customer_id,42)` |
| Projection pruning                           | Yes                                               |
| `ReadSchema`                                 | `customer_id`, `amount`                           |
| Unused `id` scanned by Spark scan            | No                                                |
| Unused `customer_name` scanned by Spark scan | No                                                |
| Columnar scan                                | Yes (`Batched: true`)                             |
| Partition filters                            | None                                              |
| Shuffle                                      | None                                              |
| Join                                         | None                                              |
| Sort                                         | None                                              |
| Result validation                            | Successful                                        |

---

# Key Takeaways

## 1. Catalyst optimization is not the whole story

Spark continues making execution decisions at the data-source boundary.

---

## 2. `PushedFilters` is a critical production signal

For Parquet scans, it tells you which supported predicates were pushed to the data source.

---

## 3. `ReadSchema` exposes projection pruning

If a large table contains 100 columns but the physical scan reports a two-column `ReadSchema`, Spark is not requesting all 100 columns from the scan.

---

## 4. Filter pushdown and partition pruning are different

A pushed filter can reduce work inside the file scan.

A partition filter can eliminate entire partitions/directories before their files are scanned.

---

## 5. Columnar execution matters

`Batched: true` followed by `ColumnarToRow` shows Spark is using a columnar scan path before converting the result to rows.

---

## 6. The physical plan is the source of truth

The production debugging principle is:

```text
Don't assume optimization.
        ↓
Inspect the plan.
        ↓
Verify the scan.
        ↓
Measure the execution.
```

---

# Module 1.4.11 Conclusion

This experiment demonstrates the transition from Catalyst-level query optimization to data-source-aware execution.

The final execution path was:

```text
Application Query
       ↓
Catalyst Analysis
       ↓
Catalyst Optimization
       ↓
Filter + Projection Requirements
       ↓
Parquet FileScan
       ├── PushedFilters
       ├── ReadSchema
       └── Batched: true
       ↓
ColumnarToRow
       ↓
Filter
       ↓
Result
```

The most important production lesson is:

> **A Spark query can be logically correct and physically valid while still performing unnecessary data-source work. Reading the `FileScan` details—especially `PushedFilters`, `ReadSchema`, and `PartitionFilters`—is how you verify whether Spark is actually minimizing the work performed by the storage layer.**

---

# Senior-Level Interview Questions

### Q1. What is predicate pushdown?

Predicate pushdown means Spark attempts to delegate supported filter predicates to the underlying data source so filtering can happen closer to where the data is stored/read.

For this experiment:

```text
PushedFilters:
[IsNotNull(customer_id), EqualTo(customer_id,42)]
```

---

### Q2. Is predicate pushdown the same as Catalyst filter optimization?

No.

Catalyst can transform the logical plan, while data-source pushdown occurs when supported predicates are communicated to the underlying source.

---

### Q3. What does `ReadSchema` tell you?

It tells you which columns the physical data-source scan needs.

Here:

```text
ReadSchema:
struct<customer_id:bigint,amount:bigint>
```

shows that only two of the four source columns are required.

---

### Q4. What is the difference between `PushedFilters` and `PartitionFilters`?

`PushedFilters` are predicates delegated to the data source/file scan.

`PartitionFilters` are predicates used to eliminate entire data partitions, often represented by directory partitioning.

---

### Q5. Does `PushedFilters` guarantee that no unnecessary disk I/O occurs?

No.

It shows that Spark successfully pushed a supported predicate to the source. The actual physical I/O reduction depends on the source implementation, file metadata, row-group statistics, data layout, compression, and predicate selectivity.

---

### Q6. Why is `ReadSchema` important for wide tables?

Because scanning fewer columns reduces the amount of data that must be read, decompressed, decoded, and processed.

This becomes particularly important for wide Parquet datasets.

---

### Q7. Why is there a `ColumnarToRow` operator?

Parquet provides columnar data to Spark's execution engine. `ColumnarToRow` converts the columnar representation into Spark's row representation when required by downstream operators.

---

### Q8. How would you investigate a slow Parquet query?

I would inspect:

```text
FileScan parquet
PushedFilters
PartitionFilters
ReadSchema
Batched
```

and then examine:

```text
file count
partition pruning
input size
shuffle boundaries
skew
task distribution
spill
```

The objective is to determine whether the problem is caused by:

```text
excessive data scanned
+
poor pruning
+
wide projection
+
small files
+
shuffle
+
skew
+
downstream computation
```

rather than assuming that the Spark engine itself is simply "slow."
