# Module 1.6.8 — Join Strategy Comparison

> Controlled strategy comparison, not a universal cost-based ranking.

| Strategy | Operator | Shuffle | Sort | Build side | Observed time* |
|---|---|---|---|---|---:|
| Broadcast | `BroadcastHashJoin` | No join shuffle | No | Customer | 0.883 s |
| Merge | `SortMergeJoin` | Both | Both | N/A | 3.742 s |
| Shuffle hash | `ShuffledHashJoin` | Both | No | Customer | 2.210 s |

All produced 10M rows. *Local Windows driver/executor observations only.*

## Engineering Takeaway
Select join strategy from data size, cardinality, memory, skew, statistics and workload behavior—not from one laptop benchmark.
