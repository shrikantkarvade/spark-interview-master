# Module 1.6.10 — AQE + Join Skew

## Objective
Demonstrate AQE detecting and splitting a skewed shuffle partition.

Workload: 10M fact rows with key 0 = 9M (90%); remaining 1M across other keys; customer = 100K; SMJ; AQE skew optimization enabled.

Final plan contained `SortMergeJoin(skew=true)` and `AQEShuffleRead`. Spark UI showed 21 AQE shuffle-read partitions, 1 skewed partition and 2 skewed partition splits. Fact AQE shuffle data was ~105.4 MiB versus ~305.2 MiB in the original shuffle; no spill.

> AQE detected the skewed shuffle partition and split it into two sub-partitions, reducing the concentration of work in the original hot partition.

> AQE mitigated the skew, but residual task imbalance remained.

## Engineering Takeaway
AQE is mitigation, not a guarantee that skew disappears completely.
