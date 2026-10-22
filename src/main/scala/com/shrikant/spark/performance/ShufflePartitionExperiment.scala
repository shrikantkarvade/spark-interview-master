package com.shrikant.spark.performance

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object ShufflePartitionExperiment {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Module 3.1 - Shuffle Architecture & Partition Tuning")
      .master("local[*]")
      .config("spark.sql.shuffle.partitions", "4") // Intentionally small to observe partitioning impact & potential bottlenecks
      .getOrCreate()

    import spark.implicits._
    println("=== SPARK SESSION INITIALIZED FOR MODULE 3.1 ===")

    // Step 1: Generate a dataset to induce a heavy shuffle
    val rowCount = 5000000L
    println(s"Generating $rowCount rows of synthetic data...")
    
    val df = spark.range(0, rowCount, 1, numPartitions = 8)
      .withColumn("group_key", $"id" % 500) // 500 distinct keys
      .withColumn("payload", concat(lit("val_"), $"id".cast("string")))

    // Step 2: Force a wide transformation (groupBy + aggregation) triggering a shuffle
    println("Executing heavy aggregation to trigger Sort-Based Shuffle...")
    val startTime = System.currentTimeMillis()

    val aggregatedDF = df.groupBy("group_key")
      .agg(
        count("id").as("total_count"),
        max("id").as("max_id")
      )

    // Trigger action and collect metrics overview
    val countResult = aggregatedDF.count()
    val endTime = System.currentTimeMillis()

    println(s"Aggregation completed in ${endTime - startTime} ms. Result partition count / rows: $countResult")

    // Step 3: Demonstrate tuning spark.sql.shuffle.partitions dynamically
    println("\n--- Demonstrating Dynamic Partition Tuning ---")
    spark.conf.set("spark.sql.shuffle.partitions", "16")
    
    val tunedDF = df.groupBy("group_key")
      .agg(sum("id").as("sum_id"))

    println(s"Tuned aggregation row count with 16 shuffle partitions: ${tunedDF.count()}")

    // Keep session alive briefly or stop cleanly
    spark.stop()
    println("=== MODULE 3.1 EXPERIMENT COMPLETED SUCCESSFULLY ===")
  }
}