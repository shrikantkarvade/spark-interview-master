# Module 1.6.9 — Join Skew

## Objective
Demonstrate how hot join keys create task imbalance.

Workload: 10M fact rows; key 0 = 5M, key 1 = 1M, key 2 = 500K, remaining = 3.5M; customer = 100K; SMJ forced.

A small number of hot keys concentrated work into shuffle partitions. Increasing `spark.sql.shuffle.partitions` alone does not split one logical key across partitions. Earlier task observations showed about 1.74× difference between slowest and faster tasks.

## Engineering Takeaway
Diagnose skew using partition-level and task-level metrics before selecting mitigation.
