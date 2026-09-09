# Module 1.6.1 — Baseline Join

## Objective
Establish the baseline Spark join execution model before applying join-specific optimizations.

## Workload
- Fact: 10,000,000 rows
- Customer dimension: 100,000 rows
- Join key: `customer_id`
- Shuffle partitions: 20
- Environment: local Windows driver/executor
- AQE: enabled

## Investigation
The baseline is the control case for broadcast, sort-merge, shuffle-hash, hints, build-side, skew and AQE experiments. The physical plan is the source of truth; Spark UI validates shuffle, task balance, sorting, memory and spill.

## Engineering Takeaway
Do not optimize a join from SQL syntax alone. Start with a physical-plan baseline and comparable runtime evidence.
