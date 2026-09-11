package com.shrikant.spark.aggregations

import com.shrikant.spark.foundation.EnterpriseSparkSession
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.desc

/**
 * Module 1.7.8 — Top-N Aggregation
 *
 * Experiment objective:
 *
 * Compare:
 *
 *   1. GROUP BY + global ORDER BY
 *   2. GROUP BY + global ORDER BY + LIMIT N
 *
 * The experiment investigates whether Spark can optimize a Top-N
 * requirement differently from a full global ordering requirement.
 *
 * Dataset:
 *   10M transactions
 *   1M customers
 *
 * Configuration:
 *   200 configured shuffle partitions
 *   AQE enabled
 *   AQE coalescing enabled
 *
 * Important:
 *
 * We deliberately inspect the ACTUAL physical plan instead of assuming
 * which Top-N operator Spark will use.
 *
 * Production questions:
 *
 *   - Do we really need the entire ordered dataset?
 *   - Can a Top-N requirement reduce work?
 *   - Does LIMIT change the physical plan?
 *   - How much data is shuffled?
 *   - Does AQE still coalesce partitions?
 *   - Does Spark perform a full Sort or a bounded Top-N operation?
 */
object TopNAggregationExperiment {

  // ---------------------------------------------------------------------------
  // Default experiment parameters
  // ---------------------------------------------------------------------------

  private val DefaultTransactionCount = 10000000L
  private val DefaultCustomerCount = 1000000L
  private val DefaultShufflePartitions = 200
  private val DefaultTopN = 100

  /**
   * Entry point.
   *
   * Supported arguments:
   *
   *   arg[0] = transaction count
   *   arg[1] = customer count
   *   arg[2] = --top-n=<N>
   *   arg[3] = --ui-pause=<seconds>
   *
   * Example:
   *
   *   ./gradlew runTopNAggregationExperiment \
   *     --args="10000000 1000000 --top-n=100 --ui-pause=300"
   */
  def main(args: Array[String]): Unit = {

    // -------------------------------------------------------------------------
    // Parse arguments
    // -------------------------------------------------------------------------

    val transactionCount =
      if (args.nonEmpty) args(0).toLong
      else DefaultTransactionCount

    val customerCount =
      if (args.length > 1) args(1).toLong
      else DefaultCustomerCount

    val topN =
      args
        .find(_.startsWith("--top-n="))
        .map(_.stripPrefix("--top-n=").toInt)
        .getOrElse(DefaultTopN)

    val uiPauseSeconds =
      args
        .find(_.startsWith("--ui-pause="))
        .map(_.stripPrefix("--ui-pause=").toInt)
        .getOrElse(0)

    // -------------------------------------------------------------------------
    // Validate input
    // -------------------------------------------------------------------------

    require(
      transactionCount > 0,
      "transactionCount must be greater than zero"
    )

    require(
      customerCount > 0,
      "customerCount must be greater than zero"
    )

    require(
      customerCount <= transactionCount,
      "customerCount must be less than or equal to transactionCount"
    )

    require(
      topN > 0,
      "topN must be greater than zero"
    )

    // -------------------------------------------------------------------------
    // Create enterprise Spark session
    // -------------------------------------------------------------------------

    val spark =
      EnterpriseSparkSession.create(
        appName = "Module 1.7.8 - Top-N Aggregation",
        environment = "dev"
      )

    // -------------------------------------------------------------------------
    // Explicitly enforce the experiment configuration.
    //
    // EnterpriseSparkSession provides the baseline environment configuration.
    // For this controlled experiment we explicitly set the shuffle and AQE
    // settings so that the experiment is reproducible.
    // -------------------------------------------------------------------------

    spark.conf.set(
      "spark.sql.shuffle.partitions",
      DefaultShufflePartitions
    )

    spark.conf.set(
      "spark.sql.adaptive.enabled",
      "true"
    )

    spark.conf.set(
      "spark.sql.adaptive.coalescePartitions.enabled",
      "true"
    )

    // -------------------------------------------------------------------------
    // Experiment header
    // -------------------------------------------------------------------------

    println()
    println("============================================================")
    println("MODULE 1.7.8 — TOP-N AGGREGATION")
    println("============================================================")
    println(s"Transaction count        : $transactionCount")
    println(s"Customer count           : $customerCount")
    println(s"Configured shuffle       : $DefaultShufflePartitions")
    println(s"AQE enabled              : ${spark.conf.get("spark.sql.adaptive.enabled")}")
    println(
      s"AQE coalescing enabled   : " +
        s"${spark.conf.get("spark.sql.adaptive.coalescePartitions.enabled")}"
    )
    println(s"Top-N                    : $topN")
    println("============================================================")
    println()

    // -------------------------------------------------------------------------
    // Generate deterministic transaction data.
    //
    // The same deterministic data is used for both runs.
    //
    // customer_id:
    //   id % customerCount
    //
    // amount:
    //   id % 1000
    //
    // This keeps the experiment reproducible and makes Run 1 vs Run 2
    // directly comparable.
    // -------------------------------------------------------------------------

    val transactions =
      spark.range(0, transactionCount)
        .selectExpr(
          "id AS transaction_id",
          s"id % $customerCount AS customer_id",
          "id % 1000 AS amount"
        )

    println(s"Input partitions       : ${transactions.rdd.getNumPartitions}")
    println()

    // =========================================================================
    // RUN 1 — FULL GLOBAL ORDER BY
    // =========================================================================

    println("------------------------------------------------------------")
    println("RUN 1 — GROUP BY + FULL GLOBAL ORDER BY")
    println("------------------------------------------------------------")

    // Fresh DataFrame for the experiment.
    //
    // The aggregation creates one final row per customer.
    val fullOrderAggregation =
      transactions
        .groupBy("customer_id")
        .sum("amount")
        .orderBy(desc("sum(amount)"))

    // -------------------------------------------------------------------------
    // Print the initial optimized physical plan.
    //
    // This shows what Spark plans before adaptive execution changes it.
    // -------------------------------------------------------------------------

    println()
    println("RUN 1 — INITIAL PHYSICAL PLAN")
    println("------------------------------------------------------------")

    fullOrderAggregation.explain(true)

    // -------------------------------------------------------------------------
    // Materialize the query.
    //
    // foreachPartition forces Spark to execute the complete query.
    // -------------------------------------------------------------------------

    val run1Start = System.nanoTime()

    fullOrderAggregation.foreachPartition {
      _: Iterator[Row] =>
        ()
    }

    val run1Seconds =
      (System.nanoTime() - run1Start) / 1e9

    println()
    println(f"RUN 1 action time        : $run1Seconds%.3f seconds")

    // -------------------------------------------------------------------------
    // Print the final executed adaptive plan.
    // -------------------------------------------------------------------------

    println()
    println("RUN 1 — FINAL EXECUTED PHYSICAL PLAN")
    println("------------------------------------------------------------")

    val run1ExecutedPlan =
      fullOrderAggregation.queryExecution.executedPlan

    println(run1ExecutedPlan.treeString)

    // -------------------------------------------------------------------------
    // Programmatic validation.
    //
    // We expect the full ORDER BY query to contain:
    //
    //   - Exchange
    //   - range partitioning
    //   - Sort
    //
    // We do not assume exact operator IDs.
    // -------------------------------------------------------------------------

    val run1PlanText =
      run1ExecutedPlan.treeString

    val run1HasExchange =
      run1PlanText.contains("Exchange")

    val run1HasRangePartitioning =
      run1PlanText.contains("rangepartitioning")

    val run1HasSort =
      run1PlanText.contains("Sort")

    println()
    println("RUN 1 — PLAN VALIDATION")
    println("------------------------------------------------------------")
    println(
      s"Exchange present       : ${if (run1HasExchange) "PASS" else "FAIL"}"
    )
    println(
      s"Range partitioning     : " +
        s"${if (run1HasRangePartitioning) "PASS" else "FAIL"}"
    )
    println(
      s"Sort present           : ${if (run1HasSort) "PASS" else "FAIL"}"
    )

    // -------------------------------------------------------------------------
    // Spark UI pause.
    //
    // IMPORTANT:
    //
    // Do not terminate the application here.
    // The pause is implemented using Thread.sleep so execution continues
    // to Run 2 after the pause.
    // -------------------------------------------------------------------------

    if (uiPauseSeconds > 0) {

      println()
      println("============================================================")
      println("Spark UI pause — RUN 1 — FULL GLOBAL ORDER BY")
      println("============================================================")
      println("Open Spark UI at: http://localhost:4040")
      println(s"Pause duration: $uiPauseSeconds seconds")
      println("Capture SQL, Stage and physical-plan metrics now.")
      println("The application will continue automatically after the pause.")
      println("============================================================")
      println()

      Thread.sleep(uiPauseSeconds * 1000L)
    }

    // =========================================================================
    // RUN 2 — TOP-N
    // =========================================================================

    println()
    println("------------------------------------------------------------")
    println(s"RUN 2 — GROUP BY + ORDER BY + LIMIT $topN")
    println("------------------------------------------------------------")

    // IMPORTANT:
    //
    // Create a fresh DataFrame for Run 2.
    //
    // This prevents caching/materialization side effects from accidentally
    // influencing the comparison.
    val topNAggregation =
      transactions
        .groupBy("customer_id")
        .sum("amount")
        .orderBy(desc("sum(amount)"))
        .limit(topN)

    // -------------------------------------------------------------------------
    // Print the initial optimized physical plan.
    // -------------------------------------------------------------------------

    println()
    println("RUN 2 — INITIAL PHYSICAL PLAN")
    println("------------------------------------------------------------")

    topNAggregation.explain(true)

    // -------------------------------------------------------------------------
    // Materialize Run 2.
    // -------------------------------------------------------------------------

    val run2Start = System.nanoTime()

    topNAggregation.foreachPartition {
      _: Iterator[Row] =>
        ()
    }

    val run2Seconds =
      (System.nanoTime() - run2Start) / 1e9

    println()
    println(f"RUN 2 action time        : $run2Seconds%.3f seconds")

    // -------------------------------------------------------------------------
    // Print final adaptive physical plan.
    // -------------------------------------------------------------------------

    println()
    println("RUN 2 — FINAL EXECUTED PHYSICAL PLAN")
    println("------------------------------------------------------------")

    val run2ExecutedPlan =
      topNAggregation.queryExecution.executedPlan

    println(run2ExecutedPlan.treeString)

    // -------------------------------------------------------------------------
    // Programmatic validation.
    //
    // We deliberately report what Spark actually produced.
    //
    // A Top-N query may use a specialized operator rather than the same
    // full-sort structure seen in Run 1.
    // -------------------------------------------------------------------------

    val run2PlanText =
      run2ExecutedPlan.treeString

    val run2HasExchange =
      run2PlanText.contains("Exchange")

    val run2HasSort =
      run2PlanText.contains("Sort")

    val run2HasTakeOrdered =
      run2PlanText.contains("TakeOrderedAndProject")

    val run2HasLimit =
      run2PlanText.contains("Limit")

    println()
    println("RUN 2 — PLAN VALIDATION")
    println("------------------------------------------------------------")
    println(
      s"Exchange present       : ${if (run2HasExchange) "YES" else "NO"}"
    )
    println(
      s"Sort present           : ${if (run2HasSort) "YES" else "NO"}"
    )
    println(
      s"TakeOrderedAndProject  : ${if (run2HasTakeOrdered) "YES" else "NO"}"
    )
    println(
      s"Limit operator         : ${if (run2HasLimit) "YES" else "NO"}"
    )

    // -------------------------------------------------------------------------
    // Optional Spark UI pause after Run 2.
    // -------------------------------------------------------------------------

    if (uiPauseSeconds > 0) {

      println()
      println("============================================================")
      println("Spark UI pause — RUN 2 — TOP-N")
      println("============================================================")
      println("Open Spark UI at: http://localhost:4040")
      println(s"Pause duration: $uiPauseSeconds seconds")
      println("Capture SQL, Stage and physical-plan metrics now.")
      println("============================================================")
      println()

      Thread.sleep(uiPauseSeconds * 1000L)
    }

    // -------------------------------------------------------------------------
    // Final experiment summary
    // -------------------------------------------------------------------------

    println()
    println("============================================================")
    println("MODULE 1.7.8 — EXPERIMENT COMPLETE")
    println("============================================================")
    println(f"Run 1 action time       : $run1Seconds%.3f seconds")
    println(f"Run 2 action time       : $run2Seconds%.3f seconds")
    println(s"Top-N                   : $topN")
    println("============================================================")

    spark.stop()
  }
}
