package com.shrikant.spark.internals.codegen

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object WholeStageCodegenExperiment {
  def main(args: Array[String]): Unit = {
    println("==================================================")
    println("Starting Module 2.3: WholeStageCodegen Experiment")
    println("==================================================")

    val spark = SparkSession.builder()
      .appName("WholeStageCodegenExperiment")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val numRows = 5000000L
    println(s"\nGenerating test dataset with $numRows rows...")
    val df = spark.range(numRows)
      .select(
        $"id",
        ($"id" % 100).as("category"),
        rand(42).as("value")
      )
      .filter($"value" > 0.1)

    // ----------------------------------------------------
    // Scenario A: WholeStageCodegen ENABLED (Default)
    // ----------------------------------------------------
    println("\n--- Scenario A: WholeStageCodegen ENABLED ---")
    spark.conf.set("spark.sql.codegen.wholeStage", "true")

    // Warm-up / Materialize
    df.count()

    val startTimeCodegen = System.currentTimeMillis()
    val resultCodegen = df.groupBy("category").agg(sum("value"), count("id")).collect()
    val durationCodegen = System.currentTimeMillis() - startTimeCodegen

    println(s"WholeStageCodegen Enabled Duration: $durationCodegen ms")
    println("Sample Physical Plan with Codegen Markers:")
    df.groupBy("category").agg(sum("value"), count("id")).explain(true)

    // ----------------------------------------------------
    // Scenario B: WholeStageCodegen DISABLED
    // ----------------------------------------------------
    println("\n--- Scenario B: WholeStageCodegen DISABLED ---")
    spark.conf.set("spark.sql.codegen.wholeStage", "false")

    val startTimeInterpreted = System.currentTimeMillis()
    val resultInterpreted = df.groupBy("category").agg(sum("value"), count("id")).collect()
    val durationInterpreted = System.currentTimeMillis() - startTimeInterpreted

    println(s"WholeStageCodegen Disabled Duration: $durationInterpreted ms")

    // ----------------------------------------------------
    // Summary Performance Comparison
    // ----------------------------------------------------
    println("\n--- Summary Performance Comparison ---")
    println(s"With Codegen    : $durationCodegen ms")
    println(s"Without Codegen : $durationInterpreted ms")
    if (durationCodegen < durationInterpreted) {
      val speedup = durationInterpreted.toDouble / durationCodegen.toDouble
      println(s"Codegen Speedup : %.2fx".format(speedup))
    }

    spark.stop()
    println("==================================================")
    println("Module 2.3 Experiment Completed Successfully.")
    println("==================================================")
  }
}