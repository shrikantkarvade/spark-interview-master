package com.shrikant.spark.performance

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object DataSkewExperiment {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Module 3.3 - Data Skew & AQE Skew Joins")
      .master("local[*]")
      .config("spark.sql.adaptive.enabled", "true")
      .config("spark.sql.adaptive.skewJoin.enabled", "true")
      .getOrCreate()

    import spark.implicits._
    println("=== SPARK SESSION INITIALIZED FOR MODULE 3.3 ===")

    // Step 1: Create a skewed dataset where key = 0 represents 80% of the rows (Severe Data Skew)
    val skewedDF = spark.range(0, 2000000, 1, numPartitions = 4)
      .withColumn("join_key", when($"id" % 10 === 0, 0L).otherwise($"id" % 500))
      .withColumn("payload", concat(lit("val_"), $"id".cast("string")))

    val dimDF = spark.range(0, 500, 1, numPartitions = 2)
      .withColumn("join_key", $"id")
      .withColumn("dim_info", concat(lit("info_"), $"id".cast("string")))

    // Step 2: Execute join under AQE Skew Join optimization
    println("\n--- Executing Skewed Join with AQE Skew Optimization Enabled ---")
    val startTime = System.currentTimeMillis()

    val joinedDF = skewedDF.join(dimDF, "join_key")
    val resultCount = joinedDF.count()

    val endTime = System.currentTimeMillis()
    println(s"Skewed join completed in ${endTime - startTime} ms. Total records: $resultCount")

    // Step 3: Demonstrate Manual Salting concept for legacy / non-AQE environments
    println("\n--- Demonstrating Manual Salting Strategy Concept ---")
    val saltFactor = 4
    val saltedSkewedDF = skewedDF
      .withColumn("salted_key", concat($"join_key", lit("_"), (rand() * saltFactor).cast("int")))

    val saltedDimDF = dimDF
      .withColumn("salt_expansion", explode(sequence(lit(0), lit(saltFactor - 1))))
      .withColumn("salted_key", concat($"join_key", lit("_"), $"salt_expansion"))

    val manualSaltedJoin = saltedSkewedDF.join(saltedDimDF, "salted_key")
    println(s"Manual salted join row count: ${manualSaltedJoin.count()}")

    spark.stop()
    println("=== MODULE 3.3 EXPERIMENT COMPLETED SUCCESSFULLY ===")
  }
}