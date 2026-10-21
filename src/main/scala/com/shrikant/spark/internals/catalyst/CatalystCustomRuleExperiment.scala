package com.shrikant.spark.internals.catalyst

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SparkSessionExtensions
import org.apache.spark.sql.functions._

class CustomRuleExtension extends (SparkSessionExtensions => Unit) {
  def apply(extensions: SparkSessionExtensions): Unit = {
    extensions.injectOptimizerRule(_ => new CustomOptimizationRule())
  }
}

object CatalystCustomRuleExperiment {
  def main(args: Array[String]): Unit = {
    println("==================================================")
    println("Starting Module 2.1: Catalyst Custom Rule Experiment")
    println("==================================================")

    val spark = SparkSession.builder()
      .appName("CatalystCustomRuleExperiment")
      .master("local[*]")
      .config("spark.sql.extensions", "com.shrikant.spark.internals.catalyst.CustomRuleExtension")
      .getOrCreate()

    import spark.implicits._

    // Use a dynamic DataFrame construction to prevent early local relation folding
    val df = spark.range(3)
      .select(
        $"id",
        when($"id" === 0, "Alice")
          .when($"id" === 1, "Bob")
          .otherwise("Charlie").as("name"),
        when($"id" === 0, "ACTIVE")
          .when($"id" === 1, "LEGACY_CODE")
          .otherwise("PENDING").as("status")
      )

    println("\n--- Executing Query with Target Predicate ---")
    val queryDF = df.filter($"status" === "LEGACY_CODE")

    // Trigger logical plan inspection
    println("\n--- Detailed Logical Execution Plans ---")
    queryDF.explain(true)

    println("\n--- Action Execution Result ---")
    queryDF.show()

    spark.stop()
    println("==================================================")
    println("Module 2.1 Experiment Completed Successfully.")
    println("==================================================")
  }
}