# Module 3.3 — Data Skew Identification, Salting, & AQE Skew Joins

## Overview
Data skew occurs when records are unevenly distributed across partition keys, causing a few "straggler" tasks to take exponentially longer than the rest. Even if 99% of tasks finish in seconds, a single skewed partition can stall the entire job, leading to poor resource utilization and potential executor OOM failures.

---

## 1. Identifying Data Skew in the Spark UI
* **Task Duration Variance:** Look at the Spark UI Stage details. A massive gap between the `Min` task duration and the `Max/75th percentile` task duration is the primary red flag for data skew.
* **Input Size / Shuffle Read Size:** Check if a single task has an exceptionally high shuffle read size compared to others.

---

## 2. Adaptive Query Execution (AQE) Skew Join Optimization
Starting with Spark 3.0, AQE automatically detects and mitigates shuffle data skew during runtime.

* **How AQE Skew Joins Work:**
    1. During shuffle read metrics evaluation, AQE identifies if any partition size exceeds median thresholds (`spark.sql.adaptive.skewJoin.skewedPartitionFactor` and `spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes`).
    2. Instead of routing the entire oversized partition to a single task, AQE **splits the skewed partition** into smaller sub-blocks and replicates the corresponding matching join partition on the other side.
    3. This parallelizes the heavy key across multiple tasks without requiring code modifications.

---

## 3. Manual Salting (Legacy / Non-AQE Workaround)
When running on older Spark versions or dealing with extremely complex non-join skew, manual salting is used:

* **The Salting Technique:**
    1. **Add Salt to Skewed Table:** Append a random integer suffix (e.g., `key_0`, `key_1`, `key_2`) to the join key of the large skewed table.
    2. **Expand Dimension Table:** Use an explosion/cross-join pattern on the smaller table to duplicate its rows across all salt variants.
    3. **Join on Salted Key:** Perform the join using the new compound salt key, distributing the heavy key across $N$ distinct partitions.

---

## 4. Interview Corner: Common Deep-Dive Questions
1. **What triggers a straggler task in Spark, and how does AQE solve it automatically?**
    * *Answer:* Stragglers are caused by uneven key distribution (data skew). AQE dynamically detects partitions exceeding size thresholds at shuffle boundaries and splits them into independent sub-tasks, balancing the workload across executors.
2. **When would you still need manual salting if AQE exists?**
    * *Answer:* When data skew occurs in non-join operations (such as heavy aggregations/group-by where partition keys concentrate heavily), or when running on legacy Spark < 3.0 clusters.

### ▶️ How to Run the Module 3.3 Experiment:
```bash
./gradlew runDataSkewExperiment
```