package com.shrikant.spark.internals.tungsten

import sun.misc.Unsafe
import java.lang.reflect.Field

case class Record(id: Long, value: Double, active: Boolean)

object MemoryTungstenExperiment {
  
  // Helper to obtain sun.misc.Unsafe instance bypassing Java 17 module restrictions
  def getUnsafe: Unsafe = {
    val field: Field = classOf[Unsafe].getDeclaredField("theUnsafe")
    field.setAccessible(true)
    field.get(null).asInstanceOf[Unsafe]
  }

  def main(args: Array[String]): Unit = {
    println("==================================================")
    println("Starting Module 2.2: Tungsten Off-Heap Memory Experiment")
    println("==================================================")

    val numRecords = 5000000L
    val unsafe = getUnsafe
    val runtime = Runtime.getRuntime

    // ----------------------------------------------------
    // Case A: Standard JVM Heap Object Allocation
    // ----------------------------------------------------
    println(s"\n--- Case A: Allocating $numRecords Records on JVM Heap ---")
    
    runtime.gc()
    Thread.sleep(500)
    val heapBefore = runtime.totalMemory() - runtime.freeMemory()

    val startTimeHeap = System.currentTimeMillis()
    val jvmHeapArray = new Array[Record](numRecords.toInt)
    for (i <- 0L until numRecords) {
      jvmHeapArray(i.toInt) = Record(i, i * 1.5, i % 2 == 0)
    }
    val endTimeHeap = System.currentTimeMillis()

    val heapAfter = runtime.totalMemory() - runtime.freeMemory()
    val heapDeltaMB = (heapAfter - heapBefore) / (1024.0 * 1024.0)
    val durationHeap = endTimeHeap - startTimeHeap

    println(s"Case A Duration       : $durationHeap ms")
    println(s"Case A JVM Heap Delta : %.2f MB".format(heapDeltaMB))

    require(jvmHeapArray.length == numRecords.toInt)

    // ----------------------------------------------------
    // Case B: Tungsten-Style Off-Heap Contiguous Allocation via Unsafe
    // ----------------------------------------------------
    println(s"\n--- Case B: Allocating $numRecords Records Off-Heap (Tungsten Style) ---")
    
    val bytesPerRecord = 24L
    val totalBytes = numRecords * bytesPerRecord

    runtime.gc()
    Thread.sleep(500)
    val heapBeforeOffHeap = runtime.totalMemory() - runtime.freeMemory()

    val startTimeOffHeap = System.currentTimeMillis()
    val memoryAddress = unsafe.allocateMemory(totalBytes)

    for (i <- 0L until numRecords) {
      val offset = memoryAddress + (i * bytesPerRecord)
      unsafe.putLong(offset, i)
      unsafe.putDouble(offset + 8, i * 1.5)
      unsafe.putByte(offset + 16, if (i % 2 == 0) 1.toByte else 0.toByte)
    }
    val endTimeOffHeap = System.currentTimeMillis()

    val heapAfterOffHeap = runtime.totalMemory() - runtime.freeMemory()
    val heapDeltaOffHeap = (heapAfterOffHeap - heapBeforeOffHeap) / (1024.0 * 1024.0)
    val durationOffHeap = endTimeOffHeap - startTimeOffHeap

    println(s"Case B Duration       : $durationOffHeap ms")
    println(s"Case B JVM Heap Delta : %.2f MB (Off-Heap Native)".format(heapDeltaOffHeap))

    unsafe.freeMemory(memoryAddress)

    // ----------------------------------------------------
    // Performance Summary
    // ----------------------------------------------------
    println("\n--- Summary Performance Comparison ---")
    println(s"JVM Heap Time       : $durationHeap ms")
    println(s"Off-Heap Unsafe Time: $durationOffHeap ms")
    val speedup = durationHeap.toDouble / durationOffHeap.toDouble
    println(s"Observed Speedup    : %.2fx".format(speedup))
    println("==================================================")
    println("Module 2.2 Experiment Completed Successfully.")
    println("==================================================")
  }
}