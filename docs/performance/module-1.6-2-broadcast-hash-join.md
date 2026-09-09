# Module 1.6.2 — Broadcast Hash Join

## Objective
Demonstrate Broadcast Hash Join (BHJ) when the dimension is below Spark's broadcast threshold.

## Result
Spark selected `BroadcastHashJoin Inner BuildRight` with a `BroadcastExchange` for the customer relation. The join avoided shuffling the fact relation and required no sort.

- Fact: 10M rows
- Customer: 100K rows
- Threshold: 10 MiB
- Customer estimated size: ~4.8 MiB
- Joined output: 10M rows

## Engineering Takeaway
A reliably small dimension can eliminate expensive join shuffle. Validate that the dimension remains safely broadcastable as data grows.
