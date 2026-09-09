# Module 1.6.5 — Shuffle Hash Join

## Objective
Demonstrate a shuffled hash join and its mechanics relative to SMJ.

Run: `./gradlew runShuffleHashJoinExperiment --args="1048576 20 false"`

Result: `ShuffledHashJoin Inner BuildRight`; both sides shuffled; no sort; 10M output rows; ~0.790 s local observation; AQE coalesced 20 → 3.

`preferSortMergeJoin=false` influences selection but is not an unconditional guarantee; eligibility and statistics still matter.

## Engineering Takeaway
SHJ removes sorting but requires an in-memory hash structure. Use it only when build-side memory is suitable.
