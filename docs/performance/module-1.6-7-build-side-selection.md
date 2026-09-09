# Module 1.6.7 — Build-Side Selection

## Objective
Understand build-side selection in hash joins.

| Strategy | Order | Build side | Observed time |
|---|---|---|---:|
| SHJ | NORMAL | BuildRight (customer) | ~2.309 s |
| SHJ | REVERSE | BuildLeft (customer) | ~2.882 s |
| BHJ | NORMAL | BuildRight (customer) | ~0.624 s |
| BHJ | REVERSE | BuildLeft (customer) | ~0.594 s |

`BuildLeft`/`BuildRight` describes the physical side, not business preference.

## Engineering Takeaway
Inspect the actual build side because it affects executor memory, GC, spill risk and failure risk.
