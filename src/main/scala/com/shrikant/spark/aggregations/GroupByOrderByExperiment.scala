package com.shrikant.spark.aggregations

import com.shrikant.spark.foundation.EnterpriseSparkSession
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object GroupByOrderByExperiment {

  // ---------------------------------------------------------------------------
  // Module 1.7.7
  //
  // Objective:
  // Understand the execution impact of combining:
  //
  //   GROUP BY customer_id
  //
  // with:
  //
  //   ORDER BY aggregate result
  //
  // The experiment compares:
  //
  //   Run 1 -> GROUP BY only
  //   Run 2 -> GROUP BY + ORDER BY
  //
  // The dataset, input partitions, shuffle partitions and aggregation logic
  // remain controlled so that the additional cost of global ordering can be
  // isolated.
  // ---------------------------------------------------------------------------

  private val DefaultTransactionCount = 10000000L
  private val DefaultCustomerCount = 1000000L
  private val DefaultShufflePartitions = 200

  def main(args: Array[String]): Unit = {

    // -------------------------------------------------------------------------
    // Parse command-line arguments
    //
    // Usage:
    //
    // ./gradlew runGroupByOrderByExperiment \
    //   --args="10000000 1000000 --ui-pause=300"
    //
    // Argument 1 = transaction count
    // Argument 2 = customer count
    // Optional  = --ui-pause=<seconds>
    // -------------------------------------------------------------------------

    val transactionCount =
      if (args.nonEmpty) args(0).toLong
      else DefaultTransactionCount

    val customerCount =
      if (args.length > 1) args(1).toLong
      else DefaultCustomerCount

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
      "Transaction count must be greater than zero."
    )

    require(
      customerCount > 0,
      "Customer count must be greater than zero."
    )

    require(
      customerCount <= transactionCount,
      "Customer count should not exceed transaction count for this experiment."
    )

    require(
      uiPauseSeconds >= 0,
      "UI pause seconds cannot be negative."
    )

    // -------------------------------------------------------------------------
    // Create Spark session using the project's standard EnterpriseSparkSession.
    // -------------------------------------------------------------------------

    val spark: SparkSession =
      EnterpriseSparkSession.create(
        "GroupByOrderByExperiment",
        "dev"
      )

    try {

      // -----------------------------------------------------------------------
      // Controlled Spark configuration
      //
      // AQE remains enabled because Module 1.7.7 is intended to study the
      // interaction between aggregation, ordering and Spark's adaptive
      // execution behavior.
      //
      // Shuffle partitions are fixed at 200 for both experiments.
      // -----------------------------------------------------------------------

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

      // -----------------------------------------------------------------------
      // Experiment header
      // -----------------------------------------------------------------------

      println()
      println("============================================================")
      println("MODULE 1.7.7 — GROUP BY + ORDER BY")
      println("============================================================")
      println(s"Transaction count        : $transactionCount")
      println(s"Customer count           : $customerCount")
      println(s"Configured shuffle       : $DefaultShufflePartitions")
      println(
        s"AQE enabled              : " +
          spark.conf.get("spark.sql.adaptive.enabled")
      )
      println(
        s"AQE coalescing enabled   : " +
          spark.conf.get("spark.sql.adaptive.coalescePartitions.enabled")
      )
      println("============================================================")
      println()

      // -----------------------------------------------------------------------
      // Create deterministic input data.
      //
      // The same deterministic dataset is recreated for every matrix entry.
      // This prevents cache/state reuse from influencing comparisons.
      // -----------------------------------------------------------------------

      def createTransactions(): DataFrame = {

        spark
          .range(0, transactionCount)
          .selectExpr(
            "id AS transaction_id",
            s"id % $customerCount AS customer_id",
            "id % 1000 AS amount"
          )
      }

      // =======================================================================
      // RUN 1 — GROUP BY ONLY
      // =======================================================================

      println()
      println("============================================================")
      println("RUN 1 — GROUP BY ONLY")
      println("============================================================")

      val transactionsGroupOnly =
        createTransactions()

      println(
        s"Input partitions = ${transactionsGroupOnly.rdd.getNumPartitions}"
      )

      val groupOnly =
        transactionsGroupOnly
          .groupBy("customer_id")
          .sum("amount")

      println()
      println("Initial physical plan:")
      println("------------------------------------------------------------")
      groupOnly.explain(true)

      val groupOnlyStart =
        System.nanoTime()

      // -----------------------------------------------------------------------
      // Materialize the complete aggregation.
      //
      // foreachPartition forces Spark to execute the query while avoiding
      // collecting the complete result into the driver.
      // -----------------------------------------------------------------------

      groupOnly.foreachPartition {
        _: Iterator[Row] =>
          ()
      }

      val groupOnlyElapsed =
        (System.nanoTime() - groupOnlyStart) / 1e9

      println()
      println(
        f"GROUP BY action time: $groupOnlyElapsed%.3f seconds"
      )

      println()
      println("Final executed physical plan:")
      println("------------------------------------------------------------")

      val groupOnlyExecutedPlan =
        groupOnly.queryExecution.executedPlan

      println(groupOnlyExecutedPlan.treeString)

      // -----------------------------------------------------------------------
      // Programmatic validation
      //
      // The generated Spark plan may contain expression IDs such as:
      //
      // customer_id#3L
      //
      // Therefore validation intentionally checks the stable portions of the
      // Exchange expression rather than requiring an exact attribute ID.
      // -----------------------------------------------------------------------

      val groupOnlyPlanString =
        groupOnlyExecutedPlan.toString

      val groupOnlyExchangeFound =
        groupOnlyPlanString.contains("Exchange") &&
          groupOnlyPlanString.contains("hashpartitioning(customer_id") &&
          groupOnlyPlanString.contains(
            s", $DefaultShufflePartitions)"
          )

      println()
      println(
        if (groupOnlyExchangeFound)
          "PLAN VALIDATION: PASS — GROUP BY Exchange configured for 200 partitions."
        else
          "PLAN VALIDATION: FAIL — Expected GROUP BY Exchange was not detected."
      )

      // -----------------------------------------------------------------------
      // Optional Spark UI pause.
      //
      // Capture SQL tab / Stage tab evidence for RUN 1 before continuing.
      // -----------------------------------------------------------------------

      pauseForSparkUI(
        spark,
        uiPauseSeconds,
        "RUN 1 — GROUP BY ONLY"
      )

      // =======================================================================
      // RUN 2 — GROUP BY + ORDER BY
      // =======================================================================

      println()
      println("============================================================")
      println("RUN 2 — GROUP BY + ORDER BY")
      println("============================================================")

      val transactionsGroupOrder =
        createTransactions()

      println(
        s"Input partitions = ${transactionsGroupOrder.rdd.getNumPartitions}"
      )

      // -----------------------------------------------------------------------
      // Important:
      //
      // First aggregate by customer.
      //
      // Then globally order the aggregated result by the aggregate value.
      //
      // orderBy() is a GLOBAL ordering operation.
      // Therefore this experiment is expected to introduce additional
      // distribution/sorting work compared with GROUP BY alone.
      // -----------------------------------------------------------------------

      val groupAndOrder =
        transactionsGroupOrder
          .groupBy("customer_id")
          .sum("amount")
          .orderBy(org.apache.spark.sql.functions.desc("sum(amount)"))

      println()
      println("Initial physical plan:")
      println("------------------------------------------------------------")
      groupAndOrder.explain(true)

      val groupAndOrderStart =
        System.nanoTime()

      // -----------------------------------------------------------------------
      // Materialize the complete globally ordered result.
      // -----------------------------------------------------------------------

      groupAndOrder.foreachPartition {
        _: Iterator[Row] =>
          ()
      }

      val groupAndOrderElapsed =
        (System.nanoTime() - groupAndOrderStart) / 1e9

      println()
      println(
        f"GROUP BY + ORDER BY action time: $groupAndOrderElapsed%.3f seconds"
      )

      println()
      println("Final executed physical plan:")
      println("------------------------------------------------------------")

      val groupAndOrderExecutedPlan =
        groupAndOrder.queryExecution.executedPlan

      println(groupAndOrderExecutedPlan.treeString)

      // -----------------------------------------------------------------------
      // Plan validation
      //
      // We expect:
      //
      //   1. Aggregation
      //   2. Exchange associated with aggregation
      //   3. Global sorting
      //
      // Spark may represent the final adaptive plan using AQE nodes, so the
      // validation intentionally checks for stable operator concepts rather
      // than requiring one exact physical-plan string.
      // -----------------------------------------------------------------------

      val groupAndOrderPlanString =
        groupAndOrderExecutedPlan.toString

      val exchangeFound =
        groupAndOrderPlanString.contains("Exchange")

      val sortFound =
        groupAndOrderPlanString.contains("Sort")

      val adaptivePlanFound =
        groupAndOrderPlanString.contains("AdaptiveSparkPlan")

      println()
      println(
        if (exchangeFound)
          "PLAN VALIDATION: PASS — Exchange detected."
        else
          "PLAN VALIDATION: FAIL — Exchange not detected."
      )

      println(
        if (sortFound)
          "PLAN VALIDATION: PASS — Sort detected."
        else
          "PLAN VALIDATION: FAIL — Sort not detected."
      )

      println(
        s"AQE plan detected       : $adaptivePlanFound"
      )

      // -----------------------------------------------------------------------
      // Final comparison
      // -----------------------------------------------------------------------

      println()
      println("============================================================")
      println("EXPERIMENT SUMMARY")
      println("============================================================")
      println(
        f"GROUP BY ONLY           : $groupOnlyElapsed%.3f seconds"
      )
      println(
        f"GROUP BY + ORDER BY     : $groupAndOrderElapsed%.3f seconds"
      )

      val additionalTime =
        groupAndOrderElapsed - groupOnlyElapsed

      val additionalPercentage =
        if (groupOnlyElapsed > 0)
          additionalTime / groupOnlyElapsed * 100.0
        else
          0.0

      println(
        f"Additional time         : $additionalTime%.3f seconds"
      )

      println(
        f"Additional time %%       : $additionalPercentage%.2f%%"
      )

      println()
      println("Key questions to investigate in Spark UI:")
      println("------------------------------------------------------------")
      println("1. How many stages are created for GROUP BY ONLY?")
      println("2. How many stages are created for GROUP BY + ORDER BY?")
      println("3. Where does the additional Exchange occur?")
      println("4. Where does the Sort occur?")
      println("5. How many shuffle partitions are generated?")
      println("6. Does AQE coalesce any shuffle partitions?")
      println("7. Does the Sort spill to disk?")
      println("8. What is the shuffle read/write volume?")
      println("9. What is the maximum task duration?")
      println("10. What is the peak execution memory?")
      println("11. Are there large differences between task durations?")
      println("12. Does the final adaptive plan contain a global Sort?")
      println("============================================================")

      // -----------------------------------------------------------------------
      // Keep Spark UI available for final inspection.
      // -----------------------------------------------------------------------

      pauseForSparkUI(
        spark,
        uiPauseSeconds,
        "RUN 2 — GROUP BY + ORDER BY"
      )

    } finally {

      // -----------------------------------------------------------------------
      // Do not immediately stop Spark when the user requested a UI pause.
      // The helper handles the pause before this point.
      // -----------------------------------------------------------------------

      spark.stop()
    }
  }

  // ===========================================================================
  // Spark UI helper
  // ===========================================================================

  private def pauseForSparkUI(
                               spark: SparkSession,
                               seconds: Int,
                               experimentName: String
                             ): Unit = {

    if (seconds > 0) {

      println()
      println("============================================================")
      println(s"Spark UI pause — $experimentName")
      println("============================================================")
      println(
        s"Open Spark UI at: ${
          spark.sparkContext.uiWebUrl.getOrElse("http: //localhost:4040")
        }"
      )
      println(s"Pause duration: $seconds seconds")
      println("Capture SQL and Stage metrics now.")
      println("============================================================")

      Thread.sleep(seconds * 1000L)
    }
  }
}