# Module 1.6.12 — Production Join Optimization

## Objective
Capstone scenario demonstrating production join decision-making from physical plans and runtime evidence.

| Scenario | Final strategy | Shuffle | Sort | AQE | Key lesson |
|---|---|---|---|---|---|
| NATURAL | BHJ via AQE | Fact-side stage | No | Yes | Runtime statistics can change strategy |
| BROADCAST | BHJ | No join shuffle | No | Yes | Small dimension can avoid join shuffle |
| MERGE | SMJ | Both sides | Both | Yes | Robust shuffle-based strategy |
| SHUFFLE_HASH | SHJ | Both sides | No | Yes | Hash build memory matters |
| SKEW | SMJ + AQE skew | Both sides | Both | Yes | AQE can split hot partitions |

## Production Decision Framework
1. Small, stable dimension → prefer BHJ when safely broadcastable.
2. Large-to-large join → generally allow Spark to choose; SMJ is a robust default.
3. Suitable smaller build relation → consider SHJ when memory is safe and avoiding sort has evidence.
4. Skewed join → validate AQE skew handling and task-level imbalance.
5. Hints → deliberate overrides, not defaults.

## Investigation Workflow
**Hypothesis → Configuration/Hint → Physical Plan → Spark UI → Benchmark → Production Validation**

## Critical Lessons
- Physical plan tells how Spark intends to execute.
- Spark UI task metrics reveal balance.
- A correct-looking plan can still hide severe skew.
- `BuildLeft`/`BuildRight` describes physical side.
- Hash build memory can exceed serialized shuffle size.
- More shuffle partitions cannot split one hot key.
- `isFinalPlan=false` before execution is initial; `true` after execution is final.
- Local Windows results are not production performance rankings.
- Comparable benchmarks require identical workloads and measurement methods.

## Engineering Decision
Do not ask only, “Which join is fastest?” Ask which strategy is safest and most efficient for the dataset, workload, memory envelope, statistics quality, skew profile and expected growth.
