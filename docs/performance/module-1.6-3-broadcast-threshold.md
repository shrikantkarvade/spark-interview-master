# Module 1.6.3 — Broadcast Threshold

## Objective
Show how `spark.sql.autoBroadcastJoinThreshold` changes physical join strategy.

| Threshold | Physical Join | Shuffle | Observed time |
|---:|---|---|---:|
| 10 MiB | Broadcast Hash Join | No join shuffle | ~0.654 s |
| 1 MiB | Sort Merge Join | Both sides | ~0.643 s |

The local timings are effectively indistinguishable and are not a meaningful performance ranking. The 1 MiB run used Exchange + Sort on both sides; AQE coalesced 20 → 3 runtime partitions.

## Engineering Takeaway
The threshold is a physical-plan decision lever. Validate the resulting plan and production metrics instead of optimizing from a tiny local benchmark.
