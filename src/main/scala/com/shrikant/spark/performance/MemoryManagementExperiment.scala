package com.shrikant.spark.performance

import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel

object MemoryManagementExperiment {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Module 3.4 - Memory Management & Tuning")
      .master("local[*]")
      .config("spark.memory.fraction", "0.6") // 60% of heap used for Unified Memory Manager (Execution + Storage)
      .config("spark.memory.storageFraction", "0.5") // 50% of unified memory reserved strictly for storage (caching)
      .getOrCreate()

    import spark.implicits._
    println("=== SPARK SESSION INITIALIZED FOR MODULE 3.4 ===")

    // Step 1: Generate dataset to test caching and memory allocation pools
    val rowCount = 1000000L
    println(s"Generating $rowCount rows for memory management demonstration...")
    
    val df = spark.range(0, rowCount, 1, numPartitions = 4)
      .withColumn("data_payload", $"id" * 2)

    // Step 2: Persist dataset using MEMORY_AND_DISK to observe Unified Memory pool interaction
    println("Persisting DataFrame with MEMORY_AND_DISK storage level...")
    df.persist(StorageLevel.MEMORY_AND_DISK)

    val cachedCount = df.count()
    println(s"Cached DataFrame row count: $cachedCount")

    // Step 3: Trigger an unpersist to free memory pools cleanly
    df.unpersist()
    println("DataFrame unpersisted, releasing storage memory pool blocks.")

    spark.stop()
    println("=== MODULE 3.4 EXPERIMENT COMPLETED SUCCESSFULLY ===")
  }
}