package com.shrikant.spark.planning

import com.shrikant.spark.foundation.EnterpriseSparkSession

object DataSourcePushdownExperiment {

  def main(args: Array[String]): Unit = {

    println()
    println("====================================================")
    println("MODULE 1.4.11 - DATA SOURCE FILTER & PROJECTION PUSHdown")
    println("====================================================")

    val spark =
      EnterpriseSparkSession.create(
        appName = "Module 1.4.11 - Data Source Pushdown",
        environment = "dev"
      )

    import spark.implicits._

    val inputPath =
      new java.io.File(
        "build/module-1.4.11/parquet/fixture.parquet"
      ).getAbsolutePath

    println()
    println("====================================================")
    println("READ PARQUET")
    println("====================================================")

    println(s"Reading from: $inputPath")

    val result =
      spark.read
        .parquet(inputPath)
        .filter($"customer_id" === 42)
        .select(
          $"customer_id",
          $"amount"
        )

    println()
    println("====================================================")
    println("EXTENDED QUERY PLAN")
    println("====================================================")

    result.explain("extended")

    println()
    println("====================================================")
    println("EXECUTION")
    println("====================================================")

    result.show(20, truncate = false)

    println()
    println("====================================================")
    println("FORMATTED PHYSICAL PLAN")
    println("====================================================")

    result.explain("formatted")

    spark.stop()
  }
}