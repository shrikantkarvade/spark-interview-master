package com.shrikant.spark.internals.aqe

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object AQEExperiment {
  def main(args: Array[String]): Unit = {
    println("==================================================")
    println("Starting Module 2.4: Adaptive Query Execution (AQE)")
    println("==================================================")

    val spark = SparkSession.builder()
      .appName("AQEExperiment")
      .master("local[*]")
      .config("spark.sql.adaptive.enabled", "true")
      .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
      .config("spark.sql.adaptive.skewJoin.enabled", "true")
      .getOrCreate()

    import spark.implicits._

    println("\nGenerating test datasets for AQE evaluation...")
    // Large dataset to trigger shuffle and partitioning
    val largeDF = spark.range(2000000L)
      .select($"id", ($"id" % 50000).as("key"), lit("large_data").as("val"))

    // Small dataset that undergoes a shuffle, making its exact size hidden from static CBO.
    // AQE evaluates shuffle output statistics at runtime and can dynamically convert Sort-Merge Join to Broadcast Hash Join!
    val smallDF = spark.range(5000L)
      .select($"id", ($"id" % 50000).as("key"), lit("small_data").as("val"))

    println("\n--- Executing Join with AQE Enabled ---")
    val joinedDF = largeDF.repartition(16, $"key")
      .join(smallDF.repartition(16, $"key"), Seq("key"))

    println("\n--- Adaptive Execution Plan Inspection ---")
    joinedDF.explain(true)

    val countResult = joinedDF.count()
    println(s"\nJoin Result Count: $countResult")

    spark.stop()
    println("==================================================")
    println("Module 2.4 Experiment Completed Successfully.")
    println("==================================================")
  }
}