```markdown
# Module 2.2 — Tungsten Binary Encoding & Unsafe Memory Layout

## Overview

Project Tungsten is Apache Spark’s high-performance memory and CPU management engine. 

Standard JVM execution stores objects as distinct heap instances. Each object carries significant overhead: a 16-byte object header (mark word + class pointer), reference pointers (8 bytes on 64-bit JVMs with compressed OOPs disabled or enabled), and padding for 8-byte word alignment. Furthermore, heavy JVM object allocation triggers frequent Garbage Collection (GC) pauses, destroying query throughput at scale.

Tungsten solves this by introducing custom binary memory layouts (`UnsafeRow`) and raw off-heap memory management via `sun.misc.Unsafe`.

This experiment investigates Tungsten memory management from an execution-engineering perspective, comparing standard JVM heap allocations against contiguous off-heap byte allocation.

The objective is to understand:

- Why standard JVM objects introduce memory bloat and GC pressure.
- How Project Tungsten encodes rows into raw binary buffers (`UnsafeRow`).
- The role of `sun.misc.Unsafe` in bypassing JVM heap limits and GC tracing.
- How variable-length data and null tracking are managed within fixed-size word layouts using bit-mapped null indicators.
- The performance impact of cache-conscious memory access and 8-byte word alignment.
- How Java 17 module encapsulation affects direct memory access.

---

1. # Experiment Objective

The experiment compares two memory allocation strategies across 5,000,000 records:

- **Case A (JVM Heap):** Standard case classes allocated on the JVM heap.
- **Case B (Tungsten Off-Heap):** Contiguous off-heap byte allocation managed directly via `sun.misc.Unsafe`.

The conceptual execution flow is:

```text
Domain Object (Case Class)
      |
      v
JVM Heap Allocation
  - 16-byte object headers
  - Object reference pointers
  - Heavy GC overhead

```

versus:

```text
Contiguous Off-Heap Buffer
  - Fixed-size word offsets
  - Variable-length byte payload
  - Bitmapped null tracking
  - 0 MB JVM Heap Delta

```

---

# 2. Benchmark Configuration

| Parameter | Value |
| --- | --- |
| Total Records | 5,000,000 |
| Execution Mode | Local |
| Memory Management | JVM Heap vs Off-Heap |
| JDK Version | Java 17 |
| Module Add-Opens | `sun.misc`, `sun.nio.ch` |
| Spark UI | `http://localhost:4040` |

---

# 3. Experiment Workflow

The benchmark application executes two distinct phases:

### 1. Case A — JVM Heap Allocation

Allocates 5,000,000 records as standard JVM case class instances, measuring wall-clock duration and resulting JVM heap delta.

### 2. Case B — Tungsten Off-Heap Allocation

Allocates equivalent records into raw off-heap memory buffers using `sun.misc.Unsafe`, measuring allocation time, memory footprint, and zero JVM heap impact.

---

# 4. Core Architecture: How UnsafeRow Works

## 4.1 Memory Layout of an UnsafeRow

An `UnsafeRow` does not deserialize individual columns into JVM objects. Instead, it represents a row as a contiguous byte array divided into two distinct regions:

1. **Fixed-Length Region:** Contains the row header (number of fields, null-ability bit-mapping) and fixed-size 8-byte words for primitive types (integers, longs, floats). If a value is a variable-length type (like a `String` or `Binary`), the fixed-length slot stores an 8-byte encoding containing the **offset** and **length** of the data.
2. **Variable-Length Region:** Stores the actual byte payloads for strings and complex structures appended immediately after the fixed-length region.

```text
+-----------------------------------+-----------------------------------+
| Null Tracking Bitmap (8 bytes)    | Column 1 (Fixed 8-byte word)      |
+-----------------------------------+-----------------------------------+
| Column 2 (Offset & Length for String) | Column 3 (Fixed 8-byte word)  |
+-----------------------------------+-----------------------------------+
| \--- Variable-length payload (Strings, Arrays, Binary) -------------\ |
+-----------------------------------------------------------------------+

```

## 4.2 Null Tracking via Bitmaps

Instead of allocating null reference pointers for every column, Tungsten uses a bit-mapped word at the head of the row. Bit $N$ indicates whether column $N$ is null. This reduces memory footprint and enables lightning-fast null checks via bitwise operations.

## 4.3 Avoiding GC Overhead

Because off-heap memory is allocated outside the Java heap via native memory allocators (`sun.misc.Unsafe.allocateMemory`), the Garbage Collector does not scan, trace, or compact these buffers. This eliminates stop-the-world GC pauses during massive shuffle and join operations.

---

# 5. Benchmark Results & Performance Findings

The benchmark execution comparing Case A and Case B yielded significant performance deltas:

* **Speedup:** Approximately **~5x faster execution** for off-heap contiguous allocation compared to standard JVM heap object creation.
* **Duration:**
* Case A (JVM Heap): ~289 ms
* Case B (Off-Heap Unsafe): ~59 ms


* **JVM Heap Delta:** **0 MB** for Case B, preventing heap exhaustion and GC thrashing.

---

# 6. Engineering Lessons

## 6.1 JVM Object Header Overhead is Massive

On a 64-bit JVM, every object incurs a 16-byte overhead. For millions of rows in a big data pipeline, object headers can easily consume more memory than the actual data payload. Contiguous binary formatting eliminates this waste.

## 6.2 Cache Locality Improves CPU Performance

Because Tungsten rows are stored contiguously in memory, CPU hardware prefetchers can load sequential rows into L1/L2 CPU caches efficiently. Standard JVM heap objects are scattered across memory pointers, causing frequent CPU cache misses.

## 6.3 Java 17 Module Encapsulation Restrictions

Directly utilizing `sun.misc.Unsafe` in modern Java versions (Java 17+) requires explicit JVM flags (`--add-opens java.base/sun.misc=ALL-UNNAMED`) because internal JDK APIs are encapsulated by default under Project Jigsaw.

---

# 7. Enterprise Use Cases for Tungsten Memory

Why do senior data engineers care about Tungsten's memory mechanics?

1. **Massive Shuffle & Sort Efficiency:** Sorting binary `UnsafeRow` buffers requires zero object deserialization. Spark sorts the 8-byte pointer/offset references directly, saving immense CPU cycles.
2. **Spill Management:** When memory execution pools exceed threshold limits, Tungsten can spill off-heap data directly to disk in its raw binary format without object conversion.
3. **Out-of-Memory (OOM) Prevention:** Off-heap allocation provides strict memory bounding, preventing unpredictable JVM `OutOfMemoryError` crashes during heavy aggregations.

---

# 8. Interview Questions

### Q1. Why are standard JVM objects inefficient for big data processing in Spark?

Because they carry a 16-byte object header, reference pointer overhead, and scatter data across non-contiguous heap memory locations, which triggers frequent Garbage Collection pauses and CPU cache misses.

---

### Q2. How does `UnsafeRow` store variable-length data like Strings?

It stores an 8-byte word in the fixed-length region containing both the memory offset and the length of the string, while the actual character byte payload resides in the variable-length region at the end of the row.

---

### Q3. How does Tungsten handle null values without wasting memory?

It uses a bit-mapped header word at the beginning of the row where each bit represents the null-status of a corresponding column, avoiding expensive null reference pointers.

---

### Q4. What is the advantage of off-heap memory management via `sun.misc.Unsafe`?

It bypasses the JVM garbage collector entirely, eliminating stop-the-world GC pauses, reducing heap pressure to 0 MB for buffered data, and allowing direct native memory allocation.

---

# 9. Key Takeaways

The experiment demonstrates four core principles of Project Tungsten:

1. **Contiguous binary memory layout (`UnsafeRow`) eliminates JVM object overhead.**
2. **Off-heap memory management prevents GC pauses during high-throughput workloads.**
3. **Bitmapped null tracking and offset-based variable encoding maximize cache efficiency.**
4. **Hardware cache locality and CPU prefetching deliver multi-fold speedups over heap traversal.**

---

# 10. Experiment Status

**Module 2.2 — Tungsten Binary Encoding & Unsafe Memory Layout: COMPLETE ✅**

Evidence collected:

* Case A vs Case B benchmark comparison (~5x speedup: 289 ms vs 59 ms).
* 0 MB JVM heap delta validation for off-heap allocation.
* Java 17 module encapsulation resolution via Gradle flags.
* Off-heap binary allocation via `sun.misc.Unsafe`.

### ▶️ How to Run the Module 2.2 Experiment:

```bash
./gradlew runTungstenMemoryExperiment
```

### Next Experiment

**Module 2.3 — WholeStageCodegen & Janino Runtime Compilation**

The next module will explore how Spark collapses multiple physical operators into single JVM loops using Janino runtime code generation to eliminate virtual function call overhead.