# Module 4.5 — Stream-to-Stream Joins & Advanced Stateful Processing (`mapGroupsWithState`)

## 🧠 Core Architectural Concepts

### 1. Stream-to-Stream Joins Architecture

Joining two streaming DataFrames (e.g., matching User Clicks with User Impressions) is fundamentally different from batch joins because both data streams are unbounded in time.

* **Mandatory Watermarks**: Watermarks **must** be defined on *both* left and right participating streams.
* **Temporal Constraints**: Stream-to-stream joins require explicit time-range bounds in the join condition (e.g., `click_time >= imp_time AND click_time <= imp_time + INTERVAL 1 HOUR`).
* **State Buffering**: Spark buffers rows from the left stream while waiting for matching right stream rows within the time window. Watermarks determine when unjoined rows can be safely purged from state buffers.
* **Join Types**: Inner joins, Left Outer joins, and Right Outer joins are supported. Outer joins *strictly* require temporal constraints so Spark knows when to emit unmatched records with null values.

### 2. Arbitrary Stateful Processing (`mapGroupsWithState`)

Standard aggregations (`groupBy().agg()`) are limited to predefined mathematical functions. `mapGroupsWithState` provides lower-level, enterprise-grade control:

* **Custom State Types**: You define custom state case classes holding arbitrary business metrics.
* **Timeouts**: Supports processing-time and event-time timeouts for cleaning up inactive entity states.
* **Fine-Grained Updates**: You explicitly dictate whether to update, remove, or preserve state per group per batch.

---

## ❓ Top Senior Interview Questions & Strategic Answers

### Q1: Why are watermarks mandatory on *both* sides of a stream-to-stream join, and what happens if records arrive outside the join time window?

**Answer:**

* **Why Both Sides**: Spark needs to track time progression independently for both streams. Without watermarks on both streams, Spark would have no mechanism to know when old state entries can be evicted from the internal join buffers, leading to infinite state accumulation and memory crashes.
* **Out-of-Window Behavior**: If a matching record arrives *after* the watermark threshold has already passed for that time bracket, Spark treats it as expired late data and drops it, meaning the join will fail to match even if the key is correct.

### Q2: Compare standard windowed aggregations with `mapGroupsWithState`. When would you choose arbitrary stateful processing?

**Answer:**

* **Standard Aggregations (`groupBy().agg()`)**: Ideal for simple metrics (counts, averages, sums) over sliding or tumbling time windows. State management and cleanup are fully automatic based on watermarks.
* **Arbitrary Stateful Processing (`mapGroupsWithState`)**: Choose this when your business logic requires **non-window-based stateful workflows**—such as sessionization (tracking user sessions that expire after 30 minutes of inactivity), fraud detection (accumulating rolling behavioral patterns across arbitrary time intervals), or complex state machines. It gives you direct programmatic control over state creation, updates, and custom timeout expirations.

### 🚀 Try It Out

Execute the experiment using Gradle:

```bash
./gradlew runStreamJoinExperiment
```