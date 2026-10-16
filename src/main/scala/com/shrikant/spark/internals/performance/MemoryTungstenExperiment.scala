package com.shrikant.spark.internals.performance

import org.apache.spark.sql.SparkSession
import org.apache.spark.unsafe.memory.MemoryAllocator
import org.apache.spark.memory.MemoryMode
import org.apache.spark.unsafe.Platform
import java.lang.management.ManagementFactory

/**
 * Module 1.9.1: Memory Management & Project Tungsten
 *
 * Demonstrates the performance and memory footprint differences between:
 * - Case A: Standard JVM Heap Object Allocation (Object header overhead, boxing, GC pressure)
 * - Case B: Tungsten-style Off-Heap Memory Allocation (Raw byte manipulation via Spark MemoryAllocator)
 *
 * Usage:
 *   ./gradlew runMemoryTungsten -Pcase=A
 *   ./gradlew runMemoryTungsten -Pcase=B
 */
object MemoryTungstenExperiment {

  case class Record(id: Long, value: Double, code: Int)

  def main(args: Array[String]): Unit = {
    // Parse case argument (--case=A or --case=B)
    val caseArg = args.find(_.startsWith("--case=")).map(_.split("=")(1)).getOrElse("A")

    val spark = SparkSession.builder()
      .appName(s"MemoryTungstenExperiment-Case-$caseArg")
      .master("local[2]")
      .config("spark.driver.memory", "2g")
      .getOrCreate()

    import spark.implicits._

    val numRecords = 5000000
    println(s"==================================================")
    println(s"Executing Module 1.9.1 Experiment: Case $caseArg")
    println(s"Target Records: $numRecords")
    println(s"==================================================")

    val startTime = System.currentTimeMillis()
    val initialMemory = getUsedMemoryMB()

    caseArg.toUpperCase match {
      case "A" =>
        println("--> [Case A] Allocating records on standard JVM Heap...")
        val heapRecords = new Array[Record](numRecords)
        for (i <- 0 until numRecords) {
          heapRecords(i) = Record(i.toLong, i * 1.5, i % 100)
        }

        val sum = heapRecords.map(_.value).sum
        println(s"[Case A] Aggregation Result: $sum")

      case "B" =>
        println("--> [Case B] Allocating records Off-Heap using Tungsten Memory Allocator...")

        val recordSize = 20L
        val totalBytes = numRecords * recordSize

        val allocator = MemoryAllocator.UNSAFE
        val memoryBlock = allocator.allocate(totalBytes)

        try {
          val baseObject = memoryBlock.getBaseObject
          val baseOffset = memoryBlock.getBaseOffset

          for (i <- 0 until numRecords) {
            val offset = baseOffset + (i * recordSize)
            Platform.putLong(baseObject, offset, i.toLong)
            Platform.putDouble(baseObject, offset + 8, i * 1.5)
            Platform.putInt(baseObject, offset + 16, i % 100)
          }

          var sum = 0.0
          for (i <- 0 until numRecords) {
            val offset = baseOffset + (i * recordSize)
            sum += Platform.getDouble(baseObject, offset + 8)
          }

          println(s"[Case B] Off-Heap Aggregation Result: $sum")
          println(s"[Case B] Allocated ${totalBytes / (1024 * 1024)} MB off-heap successfully.")

        } finally {
          allocator.free(memoryBlock)
          println("[Case B] Off-Heap memory successfully freed.")
        }

      case other =>
        throw new IllegalArgumentException(s"Unknown case: $other. Use --case=A or --case=B")
    }

    val endTime = System.currentTimeMillis()
    val finalMemory = getUsedMemoryMB()

    println(s"--------------------------------------------------")
    println(s"Execution Time: ${endTime - startTime} ms")
    println(s"Approximate Heap Memory Delta: ${finalMemory - initialMemory} MB")
    println(s"==================================================")

    spark.stop()
  }

  private def getUsedMemoryMB(): Long = {
    val runtime = Runtime.getRuntime
    runtime.gc()
    (runtime.totalMemory() - runtime.freeMemory()) / (1024 * 1024)
  }
}