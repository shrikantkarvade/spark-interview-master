```markdown
# Module 3.2 — Deep Dive into Join Strategies & Cost-Based Optimization (CBO)

## Overview
Choosing the right join strategy is one of the most critical factors determining query performance in Apache Spark. Spark’s Catalyst optimizer evaluates available statistics and configurations to select among four primary physical join strategies.

---

## 1. Primary Join Strategies in Spark

### A. Broadcast Hash Join (BHJ) — *The Gold Standard*
* **Mechanics:** The smaller dataset is collected on the driver and broadcasted to all executor nodes via torrent broadcast. Each executor holds a local in-memory hash table of the small dataset and probes it against local partitions of the large dataset.
* **Network Cost:** Zero shuffle for the large table; only network transfer of the broadcast table.
* **When Used:** Automatically chosen when one side of the join is smaller than `spark.sql.autoBroadcastJoinThreshold` (default: 10 MB in older versions, 100 MB in Spark 3+). Can also be forced using the `broadcast()` hint.

### B. Sort-Merge Join (SMJ) — *The Safe Workhorse*
* **Mechanics:** Both datasets are shuffled across the network so that rows with the same join keys land on the same partitions. Within each partition, both datasets are sorted by the join key, and a linear merge scan joins matching rows.
* **Network Cost:** Heavy shuffle of *both* tables.
* **When Used:** Default fallback when datasets are too large to broadcast, or when sorting/hashing is required. Highly stable against Out-of-Memory errors compared to older shuffle hash joins.

### C. Shuffle Hash Join (SHJ)
* **Mechanics:** Both datasets are shuffled by join key. For each partition, Spark builds an in-memory hash table from the smaller of the two shuffled partitions and probes it with the other partition.
* **When Used:** Spark chooses SHJ if the build side is significantly smaller than the stream side *after* the shuffle, and the size fits within available memory. (Often bypassed in favor of SMJ unless explicitly hinted or tuned).

### D. Broadcast Nested Loop Join (BNLJ) — *The Last Resort*
* **Mechanics:** Performs a Cartesian product style cross-check when no equi-join condition is present (e.g., joining on inequality like `a.id < b.id`).
* **Performance:** Extremely slow ($O(M \times N)$ complexity). Avoid whenever possible.

---

## 2. Cost-Based Optimization (CBO)
Spark includes a Cost-Based Optimizer that uses table and column statistics to select optimal physical plans (especially for multi-way joins and join reordering).

* **Enabling CBO:** CBO requires catalog statistics to be populated via:
  ```sql
  ANALYZE TABLE table_name COMPUTE STATISTICS FOR ALL COLUMNS;

```

* **How CBO Works:** Unlike heuristic optimization (rule-based), CBO estimates the size, cardinality, and null-count of intermediate operator outputs to choose execution plans with the lowest estimated CPU and I/O cost.

---

## 3. Interview Corner: Common Deep-Dive Questions

1. **When would a Broadcast Hash Join fail or cause an OOM?**
* *Answer:* If the estimated size of the table is misleading (e.g., compressed Parquet files expand significantly when loaded into JVM objects), or if explicit `broadcast()` hint is used on a dataset larger than executor memory, it will trigger a driver or executor `OutOfMemoryError`.


2. **How do you force a specific join strategy in Spark SQL?**
* *Answer:* Use hints like `/*+ BROADCAST(t1) */`, `/*+ MERGE(t1) */`, or `/*+ SHUFFLE_HASH(t1) */`.

### ▶️ How to Run the Module 3.2 Experiment:
```bash
./gradlew runJoinStrategyExperiment
```