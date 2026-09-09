# Module 1.6.4 — Sort Merge Join

## Objective
Understand shuffle-and-sort mechanics of Sort Merge Join (SMJ).

Run: `./gradlew runSortMergeJoinExperiment --args="1048576 20"`

Result: `SortMergeJoin Inner` with Exchange + Sort on both sides; 10M output rows; ~0.689 s local observation; AQE coalesced 20 → 3 runtime partitions; remote fetch 0 in local mode.

## Engineering Takeaway
SMJ is robust for large relations, but introduces shuffle and sorting. Production evaluation should focus on shuffle volume, sort time, task balance and spill.
