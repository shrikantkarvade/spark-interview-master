# Module 1.6.11 — AQE Join Conversion

## Objective
Demonstrate AQE converting an initial Sort Merge Join into Broadcast Hash Join using runtime statistics.

Configuration: static broadcast threshold disabled; AQE enabled; adaptive threshold 10 MiB; no hints; 20 shuffle partitions.

Initial plan: `SortMergeJoin` with two Exchanges. Final plan: `BroadcastHashJoin Inner BuildRight` with a `BroadcastQueryStage`. Runtime customer statistics were ~3.8 MiB, below the 10 MiB adaptive threshold.

## Engineering Conclusion
Static broadcast was disabled, so Spark initially planned SMJ. After runtime statistics showed the customer relation was small enough, AQE converted the final join to BHJ. Materialized shuffle stages can remain because AQE used their statistics before conversion.
