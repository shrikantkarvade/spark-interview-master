package com.shrikant.spark.planning

import com.shrikant.spark.foundation.EnterpriseSparkSession

object LogicalPlanExperiment {

  def main(args: Array[String]): Unit = {

    println()
    println("====================================================")
    println("MODULE 1.4 - LOGICAL TO PHYSICAL PLAN")
    println("====================================================")

    val spark =
      EnterpriseSparkSession.create(
        appName = "Module 1.4 - Logical Plan Experiment",
        environment = "dev"
      )

    spark.conf.set("spark.sql.shuffle.partitions", 20)
    spark.conf.set("spark.sql.adaptive.enabled", true)
    spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", true)
    spark.conf.set(
      "spark.sql.adaptive.advisoryPartitionSizeInBytes",
      1 * 1024 * 1024
    )

    import spark.implicits._

    val result =
      spark.range(1000000)
        .select(
          ($"id" % 100).as("key")
        )
        .groupBy($"key")
        .count()

    println()
    println("====================================================")
    println("EXTENDED QUERY PLAN")
    println("====================================================")

    result.explain("extended")

    println()
    println("====================================================")
    println("EXECUTION")
    println("====================================================")

    result.collect()

    println()
    println("====================================================")
    println("FINAL EXECUTED PLAN AFTER AQE")
    println("====================================================")

    result.explain("formatted")

    spark.stop()
  }
}