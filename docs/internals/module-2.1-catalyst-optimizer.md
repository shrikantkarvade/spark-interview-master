# Module 2.1 — Catalyst Optimizer Mechanics & Custom Rule Engine

## Overview

The Catalyst Optimizer is the core heart of Apache Spark SQL.

Whenever a developer writes a DataFrame transformation or a SQL query, Spark does not execute it directly. Instead, it compiles the code into an abstract syntax tree (AST) and delegates it to Catalyst, which transforms, optimizes, and plans the execution through a series of rule-based and cost-based phases.

This experiment investigates Catalyst optimization from an execution-engineering perspective by building, registering, and executing a **custom Catalyst optimization rule** using Spark’s extension SPI (`SparkSessionExtensions`).

The objective is not just to write a query, but to understand:

* How Spark transitions a query from an unresolved logical plan to an optimized physical plan.
* Where rule-based optimizations (RBO) intercept and rewrite logical expression trees.
* How Scala pattern matching and tree transformations (`transform`) operate inside Catalyst rules.
* Why premature local data folding can bypass optimization rules, and how to construct dynamic datasets that survive the optimizer.
* How enterprise data platforms inject custom rules for security, governance, and custom predicates.
* How to inspect execution plans using `explain(true)`.

---

1. # Experiment Objective



The experiment uses a dynamically generated dataset and implements a custom rule that intercepts logical `Filter` nodes during optimization.

The conceptual execution flow is:

```text
DataFrame / SQL Query
      |
      v
[1. Parsed Logical Plan] (Unresolved AST)
      |
      v
[2. Analyzed Logical Plan] (Catalog & Type Resolution)
      |
      v
[3. Optimized Logical Plan] ──► [Custom Optimization Rule] (Interception & Rewrite)
      |
      v
[4. Physical Plan Generation] (Strategy Selection & WholeStageCodegen)
      |
      v
Execution Result

```

The experiment demonstrates how custom rules can inspect expressions, evaluate conditions, and dynamically alter query execution behavior before physical planning occurs.

---

# 2. Benchmark Configuration

| Parameter | Value |
| --- | --- |
| Spark Version | 3.5.1 |
| Scala Version | 2.12.1 |
| Java Version | 17.0.2 |
| Execution Mode | Local |
| Spark Extensions Config | `spark.sql.extensions` |
| Spark UI | `http://localhost:4040` |

The experiment initializes a `SparkSession` with our custom extension builder registered via configuration:

```scala
val spark = SparkSession.builder()
  .appName("CatalystCustomRuleExperiment")
  .master("local[*]")
  .config("spark.sql.extensions", "com.shrikant.spark.internals.catalyst.CustomRuleExtension")
  .getOrCreate()

```

---

# 3. Experiment Workflow

The application performs three sequential steps:

### 1. Dynamic Dataset Generation

A dataset is created using `spark.range()` and conditional transformations to prevent premature local evaluation:

```scala
val df = spark.range(3)
  .select(
    $"id",
    when($"id" === 0, "Alice")
      .when($"id" === 1, "Bob")
      .otherwise("Charlie").as("name"),
    when($"id" === 0, "ACTIVE")
      .when($"id" === 1, "LEGACY_CODE")
      .otherwise("PENDING").as("status")
  )

```

### 2. Query Execution with Target Predicate

A filter is applied targeting the `"LEGACY_CODE"` status:

```scala
val queryDF = df.filter($"status" === "LEGACY_CODE")

```

### 3. Plan Inspection & Action

Spark prints the detailed logical execution plans via `explain(true)` and executes an action (`show()`) to trigger compilation and execution.

---

# 4. The Four Phases of Catalyst

During execution, Catalyst processes the query through four distinct phases:

## 4.1 Parsed Logical Plan (Unresolved)

Spark parses the syntax into a logical tree. Column and table names are treated as strings; types and catalog entries are not yet verified.

Example output:

```text
== Parsed Logical Plan ==
'Filter ('status = LEGACY_CODE)
+- Project [id#0L, CASE WHEN (id#0L = cast(0 as bigint)) THEN Alice WHEN (id#0L = cast(1 as bigint)) THEN Bob ELSE Charlie END AS name#2, CASE WHEN (id#0L = cast(0 as bigint)) THEN ACTIVE WHEN (id#0L = cast(1 as bigint)) THEN LEGACY_CODE ELSE PENDING END AS status#3]
   +- Range (0, 3, step=1, splits=Some(16))

```

## 4.2 Analyzed Logical Plan (Resolved)

The Analyzer consults the Catalog to resolve data types, bind column references, and verify schema correctness.

Example output:

```text
== Analyzed Logical Plan ==
id: bigint, name: string, status: string
Filter (status#3 = LEGACY_CODE)
+- Project [id#0L, CASE WHEN (id#0L = cast(0 as bigint)) THEN Alice WHEN (id#0L = cast(1 as bigint)) THEN Bob ELSE Charlie END AS name#2, CASE WHEN (id#0L = cast(0 as bigint)) THEN ACTIVE WHEN (id#0L = cast(1 as bigint)) THEN LEGACY_CODE ELSE PENDING END AS status#3]
   +- Range (0, 3, step=1, splits=Some(16))

```

## 4.3 Optimized Logical Plan (Rule-Based Optimization)

The core optimizer applies rule batches (`Rule[LogicalPlan]`), including our custom extension rule, to simplify expressions, push down filters, and prune projections.

Example output:

```text
== Optimized Logical Plan ==
Project [id#0L, CASE WHEN (id#0L = 0) THEN Alice WHEN (id#0L = 1) THEN Bob ELSE Charlie END AS name#2, CASE WHEN (id#0L = 0) THEN ACTIVE WHEN (id#0L = 1) THEN LEGACY_CODE ELSE PENDING END AS status#3]
+- Filter CASE WHEN (id#0L = 0) THEN false WHEN (id#0L = 1) THEN true ELSE false END
   +- Range (0, 3, step=1, splits=Some(16))

```

## 4.4 Physical Plan Generation

Strategies evaluate the optimized logical plan to produce physical operators ready for execution.

Example output:

```text
== Physical Plan ==
*(1) Project [id#0L, CASE WHEN (id#0L = 0) THEN Alice WHEN (id#0L = 1) THEN Bob ELSE Charlie END AS name#2, CASE WHEN (id#0L = 0) THEN ACTIVE WHEN (id#0L = 1) THEN LEGACY_CODE ELSE PENDING END AS status#3]
+- *(1) Filter CASE WHEN (id#0L = 0) THEN false WHEN (id#0L = 1) THEN true ELSE false END
   +- *(1) Range (0, 3, step=1, splits=16)

```

---

# 5. Engineering Lessons

## 5.1 Static Local Datasets Eagerly Fold

When a DataFrame is created directly from a static Scala sequence (`Seq(...).toDF`), Spark's optimizer can evaluate and fold filters at compile time into a static `LocalRelation`, completely bypassing optimization rule batches.
Using dynamic generators like `spark.range()` ensures logical nodes survive into the Optimizer phase.

## 5.2 Pattern Matching is the Core of Rule Design

Catalyst rules rely on Scala case class extractors (`Filter`, `EqualTo`, `AttributeReference`) to pattern match nodes within immutable trees. This declarative approach allows complex query rewrites to be written concisely and safely.

## 5.3 Enterprise Extensibility via SPI

Spark's `SparkSessionExtensions` mechanism allows engineers to inject custom rules, strategies, and parsers without recompiling the core Spark engine. This is essential for enterprise data platforms.

---

# 6. Enterprise Use Cases for Custom Rules

Why do senior data platform engineers implement custom Catalyst rules in enterprise environments?

1. **Dynamic Data Masking & Governance:** Automatically injecting redaction or hashing expressions for PII columns based on user context.
2. **Multi-Tenant Row-Level Security (RLS):** Automatically appending tenant ID filters (`WHERE tenant_id = current_user_tenant()`) to all user queries against enterprise data lakes.
3. **Proprietary Predicate Pushdown:** Rewriting legacy query constructs into optimized source-specific predicates that storage formats (like Apache Iceberg or Delta Lake) can process efficiently.

---

# 7. Interview Questions

### Q1. What are the four phases of the Catalyst Optimizer?

1. Parsed Logical Plan (Unresolved AST)
2. Analyzed Logical Plan (Resolved types and catalog bindings)
3. Optimized Logical Plan (Rule-based and cost-based optimizations)
4. Physical Plan (Strategy selection for execution)

---

### Q2. How does Catalyst represent query plans internally?

As immutable trees composed of case classes (`TreeNode`) representing logical and physical operators and expressions.

---

### Q3. How can you inject custom optimization rules into Spark?

By implementing `SparkSessionExtensions` and registering rule builders via configuration:

```scala
spark.sql.extensions = com.shrikant.spark.internals.catalyst.CustomRuleExtension

```

---

### Q4. Why did a static Scala `Seq` collection bypass our custom optimization rule?

Because Spark's optimizer performed early constant folding and local evaluation, optimizing the query into a `LocalTableScan` before the custom optimization rule batch executed.

---

# 8. Key Takeaways

The experiment demonstrates four core principles of Catalyst internals:

1. **Queries are compiled into immutable logical trees before physical execution.**
2. **The Catalyst optimizer applies batches of rule-based transformations (`Rule[LogicalPlan]`).**
3. **Custom rules can be safely injected via `SparkSessionExtensions` for enterprise governance and optimization.**
4. **Validating plan evolution via `explain(true)` is essential for understanding engine behavior.**

---

# 9. Experiment Status

**Module 2.1 — Catalyst Optimizer Mechanics & Custom Rule Engine: COMPLETE ✅**

Evidence collected:

* Custom Catalyst rule interception during logical plan optimization.
* Session extension builder integration.
* Experiment harness execution via custom Gradle task.
* Console log verification of rule evaluation across query execution phases.

### ▶️ How to Run the Module 2.2 Experiment:

```bash
./gradlew runCatalystCustomRule
```

### Next Experiment

**Module 2.2 — Tungsten Binary Encoding & Unsafe Memory Layout**

The next module will move down the execution stack to investigate how Catalyst rows are serialized into cache-conscious binary formats (`UnsafeRow`) and managed off-heap via `sun.misc.Unsafe`.