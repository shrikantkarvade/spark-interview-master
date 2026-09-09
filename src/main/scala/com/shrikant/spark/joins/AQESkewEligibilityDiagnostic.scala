package com.shrikant.spark.joins

import com.shrikant.spark.foundation.EnterpriseSparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

object AQESkewEligibilityDiagnostic {

  def main(args: Array[String]): Unit = {

    val shufflePartitions =
      if (args.length > 0) args(0).toInt else 20

    val skewFactor =
      if (args.length > 1) args(1).toDouble else 2.0

    val skewThreshold =
      if (args.length > 2) args(2).toLong else 1024 * 1024

    val uiPauseSeconds =
      args
        .find(_.startsWith("--ui-pause="))
        .map(_.stripPrefix("--ui-pause=").toInt)
        .getOrElse(0)

    val spark: SparkSession =
      EnterpriseSparkSession.create(
        "AQE Skew Eligibility Diagnostic",
        "dev"
      )

    // -----------------------------------------------------------------------
    // AQE configuration
    // -----------------------------------------------------------------------

    spark.conf.set(
      "spark.sql.shuffle.partitions",
      shufflePartitions
    )

    // Disable broadcast so that the experiment is forced through
    // a shuffled join.
    spark.conf.set(
      "spark.sql.autoBroadcastJoinThreshold",
      -1
    )

    spark.conf.set(
      "spark.sql.adaptive.enabled",
      true
    )

    spark.conf.set(
      "spark.sql.adaptive.skewJoin.enabled",
      true
    )

    spark.conf.set(
      "spark.sql.adaptive.skewJoin.skewedPartitionFactor",
      skewFactor.toString
    )

    spark.conf.set(
      "spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes",
      skewThreshold.toString
    )

    // Keep reducer count stable so that we can clearly observe
    // whether AQE itself performs skew partition splitting.
    spark.conf.set(
      "spark.sql.adaptive.coalescePartitions.enabled",
      false
    )

    // Force OptimizeSkewedJoin even when Spark estimates that
    // an additional shuffle may be required.
    spark.conf.set(
      "spark.sql.adaptive.forceOptimizeSkewedJoin",
      true
    )

    // -----------------------------------------------------------------------
    // Configuration diagnostics
    // -----------------------------------------------------------------------

    println()
    println("=" * 80)
    println("MODULE 1.6.10A - AQE SKEW ELIGIBILITY DIAGNOSTIC")
    println("=" * 80)

    println()
    println("Spark version                  = " + spark.version)
    println(
      "spark.sql.shuffle.partitions  = " +
        spark.conf.get("spark.sql.shuffle.partitions")
    )
    println(
      "autoBroadcastJoinThreshold    = " +
        spark.conf.get("spark.sql.autoBroadcastJoinThreshold")
    )
    println(
      "adaptive.enabled               = " +
        spark.conf.get("spark.sql.adaptive.enabled")
    )
    println(
      "skewJoin.enabled               = " +
        spark.conf.get("spark.sql.adaptive.skewJoin.enabled")
    )
    println(
      "skewJoin.skewedPartitionFactor = " +
        spark.conf.get(
          "spark.sql.adaptive.skewJoin.skewedPartitionFactor"
        )
    )
    println(
      "skewJoin.threshold             = " +
        spark.conf.get(
          "spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes"
        ) +
        " bytes"
    )
    println(
      "coalescePartitions.enabled     = " +
        spark.conf.get(
          "spark.sql.adaptive.coalescePartitions.enabled"
        )
    )
    println(
      "forceOptimizeSkewedJoin        = " +
        spark.conf.get(
          "spark.sql.adaptive.forceOptimizeSkewedJoin"
        )
    )

    // -----------------------------------------------------------------------
    // Create intentionally skewed fact data
    //
    // 10M total rows
    //
    // customer_id = 0
    //   9M rows -> 90%
    //
    // customer_id = 1..99999
    //   1M rows distributed across remaining keys
    // -----------------------------------------------------------------------

    println()
    println("-" * 80)
    println("Creating skewed fact dataset")
    println("-" * 80)

    val factDf: DataFrame =
      spark.range(0, 10000000)
        .select(
          col("id").alias("transaction_id"),

          when(
            col("id") < 9000000,
            lit(0)
          ).otherwise(
            ((col("id") - 9000000) % 99999) + 1
          ).alias("customer_id"),

          (col("id") % 1000).alias("amount")
        )

    // -----------------------------------------------------------------------
    // Customer dimension
    // -----------------------------------------------------------------------

    val customerDf: DataFrame =
      spark.range(0, 100000)
        .select(
          col("id").alias("customer_id"),
          concat(
            lit("CUSTOMER_"),
            col("id")
          ).alias("customer_name")
        )

    println("Fact rows expected     = 10,000,000")
    println("Customer rows expected = 100,000")
    println("Hot key                = customer_id=0")
    println("Hot key rows           = 9,000,000")
    println("Hot key percentage     = 90%")

    // -----------------------------------------------------------------------
    // Verify logical skew
    //
    // This is a diagnostic action.
    // It is intentionally performed BEFORE the join benchmark.
    // Its Spark UI jobs should NOT be included in the join benchmark.
    // -----------------------------------------------------------------------

    println()
    println("-" * 80)
    println("Logical skew verification")
    println("-" * 80)

    factDf
      .groupBy("customer_id")
      .count()
      .orderBy(desc("count"))
      .show(5, truncate = false)

    // -----------------------------------------------------------------------
    // Force Sort Merge Join.
    //
    // BROADCAST is disabled globally.
    // MERGE hints explicitly request SMJ.
    // -----------------------------------------------------------------------

    val joinedDf =
      factDf
        .hint("MERGE")
        .join(
          customerDf.hint("MERGE"),
          Seq("customer_id"),
          "inner"
        )
        .select(
          col("transaction_id"),
          col("customer_id"),
          col("amount"),
          col("customer_name")
        )

    // -----------------------------------------------------------------------
    // Initial physical plan
    //
    // IMPORTANT:
    // This is the plan BEFORE execution.
    // AdaptiveSparkPlan will normally report:
    //
    //   isFinalPlan=false
    //
    // at this point.
    // -----------------------------------------------------------------------

    println()
    println("=" * 80)
    println("INITIAL PHYSICAL PLAN")
    println("=" * 80)

    joinedDf.explain("formatted")

    // -----------------------------------------------------------------------
    // Pure join execution
    //
    // IMPORTANT:
    //
    // Do NOT use count() here.
    //
    // count() introduces:
    //
    //   HashAggregate
    //        |
    //      Exchange
    //
    // after the join.
    //
    // foreachPartition consumes the joined rows without introducing
    // an aggregation stage, giving us a cleaner view of the join itself.
    // -----------------------------------------------------------------------

    println()
    println("=" * 80)
    println("EXECUTING JOIN")
    println("=" * 80)

    val start = System.nanoTime()

    joinedDf.foreachPartition { rows: Iterator[org.apache.spark.sql.Row] =>
      while (rows.hasNext) {
        rows.next()
      }
    }

    val elapsedSeconds =
      (System.nanoTime() - start) / 1e9

    println()
    println(
      f"Join execution time = $elapsedSeconds%.3f seconds"
    )

    // -----------------------------------------------------------------------
    // Final adaptive physical plan
    //
    // This is the important diagnostic.
    //
    // We expect:
    //
    // AdaptiveSparkPlan
    // +- == Final Plan ==
    //    * Project
    //    +- * SortMergeJoin
    //       :- * Sort
    //       :  +- ShuffleQueryStage
    //       :     +- Exchange
    //       +- * Sort
    //          +- ShuffleQueryStage
    //             +- Exchange
    //
    // If AQE skew optimization activates, we should see structures
    // such as:
    //
    //   CustomShuffleReader
    //   PartialReducerPartitionSpec
    //
    // -----------------------------------------------------------------------

    println()
    println("=" * 80)
    println("FINAL ADAPTIVE PHYSICAL PLAN")
    println("=" * 80)

    val finalPlan =
      joinedDf.queryExecution.executedPlan

    println(finalPlan.toString)

    // -----------------------------------------------------------------------
    // AQE skew detection
    // -----------------------------------------------------------------------

    val finalPlanString =
      finalPlan.toString

    val hasCustomShuffleReader =
      finalPlanString.contains("CustomShuffleReader")

    val hasPartialReducerPartitionSpec =
      finalPlanString.contains("PartialReducerPartitionSpec")

    val hasOptimizeSkewedJoin =
      finalPlanString.contains("OptimizeSkewedJoin")

    println()
    println("=" * 80)
    println("AQE SKEW OPTIMIZATION DETECTION")
    println("=" * 80)

    println(
      "CustomShuffleReader          = " +
        hasCustomShuffleReader
    )

    println(
      "PartialReducerPartitionSpec  = " +
        hasPartialReducerPartitionSpec
    )

    println(
      "OptimizeSkewedJoin            = " +
        hasOptimizeSkewedJoin
    )

    // -----------------------------------------------------------------------
    // Engineering conclusion
    // -----------------------------------------------------------------------

    println()
    println("=" * 80)
    println("ENGINEERING CONCLUSION")
    println("=" * 80)

    if (
      hasCustomShuffleReader ||
        hasPartialReducerPartitionSpec ||
        hasOptimizeSkewedJoin
    ) {

      println(
        "AQE skew optimization appears in the final physical plan."
      )

      println(
        "Spark identified eligible skewed shuffle partitions "
          + "and modified the execution plan."
      )

    } else {

      println(
        "AQE is enabled and produced a final adaptive plan, "
          + "but no explicit skew-partition splitting structure "
          + "is visible in the final physical plan."
      )

      println(
        "Inspect Spark UI task-duration and shuffle-partition "
          + "metrics before concluding whether the skew remains "
          + "the dominant execution bottleneck."
      )
    }

    println()
    println("=" * 80)

    // -----------------------------------------------------------------------
    // Optional Spark UI pause
    // -----------------------------------------------------------------------

    if (uiPauseSeconds > 0) {

      println()
      println(
        s"Spark UI available. Keeping application alive for " +
          s"$uiPauseSeconds seconds..."
      )

      Thread.sleep(uiPauseSeconds * 1000L)
    }

    spark.stop()
  }
}
