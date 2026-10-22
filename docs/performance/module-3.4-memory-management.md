# Module 3.4 — Memory Management, Execution/Storage Pools, & GC Tuning

## Overview
Managing executor memory effectively is vital to preventing `OutOfMemoryError` (OOM) crashes and excessive Garbage Collection (GC) pauses. Spark uses a **Unified Memory Manager** that allows execution and storage memory pools to dynamically borrow from one another.

---

## 1. Unified Memory Manager Architecture
Unlike legacy Spark versions where memory allocations were strictly partitioned, modern Spark divides the JVM heap (`spark.executor.memory`) into distinct regions:

1. **Reserved Memory:** Fixed 300 MB reserved strictly for Spark internal system overhead.
2. **Usable Memory:** The remaining heap space, which is split by `spark.memory.fraction` (default `0.6` or 60%) into **Unified Memory**, with the rest left for user data structures and JVM metadata.
3. **Execution vs. Storage Pools:**
    * **Execution Memory:** Used for shuffles, joins, sorts, and aggregations.
    * **Storage Memory:** Used for caching DataFrames (`df.cache()`) and broadcast variables.
    * **Dynamic Boundary:** Execution and storage share the same unified pool boundary. If storage needs space, it can borrow from execution (if execution isn't using it). Conversely, execution can evict cached blocks from storage if it runs out of memory.

---

## 2. Garbage Collection (GC) Tuning & Avoidance
Large JVM heap sizes often lead to long "Stop-the-World" Garbage Collection pauses, causing executors to drop out of heartbeats and fail.

* **Best Practices for GC Mitigation:**
    * **Leverage Tungsten Off-Heap Memory:** By storing binary data off-heap (`spark.memory.offHeap.enabled = true`), you bypass JVM object headers and drastically reduce heap pressure and GC overhead.
    * **Use G1GC:** For executors with large heaps (> 32 GB), G1GC (`-XX:+UseG1GC`) is recommended over traditional Parallel GC to maintain predictable pause times.
    * **Avoid Unnecessary Caching:** Caching intermediate datasets that are only used once consumes valuable storage memory and triggers eviction thrashing.

---

## 3. Interview Corner: Common Deep-Dive Questions
1. **How do Execution and Storage memory pools interact in Spark?**
    * *Answer:* They share a unified memory boundary (`spark.memory.fraction`). They can borrow freely from each other up to their respective limits (`spark.memory.storageFraction`), but execution can evict storage blocks if needed, whereas storage cannot evict active execution memory.
2. **What causes executor loss due to GC pauses, and how do you fix it?**
    * *Answer:* Massive JVM heaps overloaded with millions of boxed Java objects cause long GC pauses. The fixes are reducing object overhead using Project Tungsten/Unsafe encoders, enabling off-heap memory, or switching to G1GC.

### ▶️ How to Run the Module 3.3 Experiment:
```bash
./gradlew runMemoryManagementExperiment
```