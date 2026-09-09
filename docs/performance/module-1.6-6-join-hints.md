# Module 1.6.6 — Join Hints

## Objective
Demonstrate explicit join hints as engineering overrides.

| Hint | Physical Join | Shuffle | Sort | Observed time |
|---|---|---|---|---:|
| `BROADCAST` | BHJ BuildRight | No join shuffle | No | ~0.674 s |
| `MERGE` | SMJ | Both sides | Both | ~4.742 s |
| `SHUFFLE_HASH` | SHJ BuildRight | Both sides | No | ~2.235 s |

All produced 10M rows.

## Engineering Rule
Hints are useful when reliable domain knowledge is available, but become dangerous when data grows.

**Hint → Validate physical plan → Measure → Monitor**
