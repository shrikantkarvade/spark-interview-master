package com.shrikant.spark.performance

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object JoinStrategyExperiment {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Module 3.2 - Join Strategies & CBO")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._
    println("=== SPARK SESSION INITIALIZED FOR MODULE 3.2 ===")

    // Step 1: Create a large fact dataset and a small dimension dataset
    val largeDF = spark.range(0, 1000000, 1, numPartitions = 4)
      .withColumn("join_key", $"id" % 10000)
      .withColumn("fact_value", concat(lit("fact_"), $"id".cast("string")))

    val smallDF = spark.range(0, 500, 1, numPartitions = 2)
      .withColumn("join_key", $"id")
      .withColumn("dim_desc", concat(lit("desc_"), $"id".cast("string")))

    // Step 2: Default join execution (Small table size is under 10MB default threshold -> Broadcast Hash Join)
    println("\n--- Case A: Default Behavior (Expected: BroadcastHashJoin) ---")
    val defaultJoin = largeDF.join(smallDF, "join_key")
    println(defaultJoin.queryExecution.executedPlan.toString())
    println(s"Default join result count: ${defaultJoin.count()}")

    // Step 3: Disable broadcast join by setting threshold to -1 (Forcing Sort-Merge Join / Shuffle Hash Join)
    println("\n--- Case B: Forced Non-Broadcast (spark.sql.autoBroadcastJoinThreshold = -1) ---")
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")

    val forcedJoin = largeDF.join(smallDF, "join_key")
    println(forcedJoin.queryExecution.executedPlan.toString())
    println(s"Forced non-broadcast join result count: ${forcedJoin.count()}")

    spark.stop()
    println("=== MODULE 3.2 EXPERIMENT COMPLETED SUCCESSFULLY ===")
  }
}