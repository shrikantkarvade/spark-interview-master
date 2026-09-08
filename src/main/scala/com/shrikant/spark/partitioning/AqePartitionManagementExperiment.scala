package com.shrikant.spark.partitioning

import com.shrikant.spark.foundation.EnterpriseSparkSession
import org.apache.spark.sql.functions._

object AqePartitionManagementExperiment {

  private val NumRows = 1000000
  private val NumShufflePartitions = 20
  private val AdvisoryPartitionSize = 1024 * 1024

  def main(args: Array[String]): Unit = {

    println()
    println("====================================================")
    println("MODULE 1.5.9 - AQE PARTITION MANAGEMENT")
    println("====================================================")

    val spark =
      EnterpriseSparkSession.create(
        appName = "Module 1.5.9 - AQE Partition Management",
        environment = "dev"
      )

    import spark.implicits._

    /*
     * Keep the shuffle configuration identical for both
     * experiments so that AQE is the primary variable.
     */
    spark.conf.set(
      "spark.sql.shuffle.partitions",
      NumShufflePartitions
    )

    spark.conf.set(
      "spark.sql.adaptive.coalescePartitions.enabled",
      true
    )

    spark.conf.set(
      "spark.sql.adaptive.advisoryPartitionSizeInBytes",
      AdvisoryPartitionSize
    )

    val df =
      spark.range(NumRows)
        .select(
          ($"id" % 100).as("key")
        )

    println()
    println("====================================================")
    println("EXPERIMENT CONFIGURATION")
    println("====================================================")

    println(
      s"spark.master = ${spark.sparkContext.master}"
    )

    println(
      s"Input rows = $NumRows"
    )

    println(
      s"Input partitions = ${df.rdd.getNumPartitions}"
    )

    println(
      s"spark.sql.shuffle.partitions = " +
        spark.conf.get("spark.sql.shuffle.partitions")
    )

    println(
      s"spark.sql.adaptive.coalescePartitions.enabled = " +
        spark.conf.get(
          "spark.sql.adaptive.coalescePartitions.enabled"
        )
    )

    println(
      s"spark.sql.adaptive.advisoryPartitionSizeInBytes = " +
        spark.conf.get(
          "spark.sql.adaptive.advisoryPartitionSizeInBytes"
        )
    )

    /*
     * ==================================================
     * EXPERIMENT 1 - AQE ENABLED
     * ==================================================
     */
    println()
    println("====================================================")
    println("EXPERIMENT 1 - AQE ENABLED")
    println("====================================================")

    spark.conf.set(
      "spark.sql.adaptive.enabled",
      true
    )

    println(
      s"spark.sql.adaptive.enabled = " +
        spark.conf.get("spark.sql.adaptive.enabled")
    )

    val aqeEnabledResult =
      df.groupBy($"key")
        .count()

    println()
    println("INITIAL PLAN - AQE ENABLED")
    println("----------------------------------------------------")

    aqeEnabledResult.explain("formatted")

    val aqeEnabledStart =
      System.nanoTime()

    val aqeEnabledRows =
      aqeEnabledResult.collect()

    val aqeEnabledElapsedMs =
      (System.nanoTime() - aqeEnabledStart) / 1000000

    println()
    println("FINAL PLAN - AQE ENABLED")
    println("----------------------------------------------------")

    aqeEnabledResult.explain("formatted")

    println()
    println("AQE ENABLED RESULT")
    println("----------------------------------------------------")

    println(
      s"Result rows = ${aqeEnabledRows.length}"
    )

    println(
      s"Execution time = ${aqeEnabledElapsedMs} ms"
    )

    /*
     * ==================================================
     * EXPERIMENT 2 - AQE DISABLED
     * ==================================================
     */
    println()
    println("====================================================")
    println("EXPERIMENT 2 - AQE DISABLED")
    println("====================================================")

    spark.conf.set(
      "spark.sql.adaptive.enabled",
      false
    )

    spark.conf.set(
      "spark.sql.adaptive.coalescePartitions.enabled",
      false
    )

    println(
      s"spark.sql.adaptive.enabled = " +
        spark.conf.get("spark.sql.adaptive.enabled")
    )

    println(
      s"spark.sql.adaptive.coalescePartitions.enabled = " +
        spark.conf.get(
          "spark.sql.adaptive.coalescePartitions.enabled"
        )
    )

    val aqeDisabledResult =
      df.groupBy($"key")
        .count()

    println()
    println("INITIAL PLAN - AQE DISABLED")
    println("----------------------------------------------------")

    aqeDisabledResult.explain("formatted")

    val aqeDisabledStart =
      System.nanoTime()

    val aqeDisabledRows =
      aqeDisabledResult.collect()

    val aqeDisabledElapsedMs =
      (System.nanoTime() - aqeDisabledStart) / 1000000

    println()
    println("FINAL PLAN - AQE DISABLED")
    println("----------------------------------------------------")

    aqeDisabledResult.explain("formatted")

    println()
    println("AQE DISABLED RESULT")
    println("----------------------------------------------------")

    println(
      s"Result rows = ${aqeDisabledRows.length}"
    )

    println(
      s"Execution time = ${aqeDisabledElapsedMs} ms"
    )

    /*
     * ==================================================
     * CORRECTNESS
     * ==================================================
     */
    println()
    println("====================================================")
    println("CORRECTNESS")
    println("====================================================")

    println(
      s"AQE ON result rows  = ${aqeEnabledRows.length}"
    )

    println(
      s"AQE OFF result rows = ${aqeDisabledRows.length}"
    )

    println(
      s"Counts equal = ${
        aqeEnabledRows.length == aqeDisabledRows.length
      }"
    )

    /*
     * ==================================================
     * COMPARISON
     * ==================================================
     */
    println()
    println("====================================================")
    println("AQE COMPARISON")
    println("====================================================")

    println(
      s"AQE ON  execution time = ${aqeEnabledElapsedMs} ms"
    )

    println(
      s"AQE OFF execution time = ${aqeDisabledElapsedMs} ms"
    )

    println()
    println("Expected physical-plan difference:")
    println(
      "AQE ON  -> AdaptiveSparkPlan + AQEShuffleRead " +
        "(coalesced)"
    )
    println(
      "AQE OFF -> Static physical plan without " +
        "AdaptiveSparkPlan/AQEShuffleRead"
    )

    println()
    println("AQE partition-management experiment completed.")

    spark.stop()
  }
}
