package com.shrikant.spark.aggregations

import com.shrikant.spark.foundation.EnterpriseSparkSession
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions._

/**
 * Module 1.7.9 — Data Skew in Aggregation
 *
 * Experiment Objective:
 *
 * Demonstrate how a highly skewed GROUP BY key distribution affects
 * Spark aggregation and shuffle execution.
 *
 * Two controlled experiments are executed:
 *
 * Run 1:
 * Uniform customer distribution
 *
 * Run 2:
 * Highly skewed customer distribution
 *
 * The experiment intentionally focuses on identifying the skew problem.
 * Skew mitigation techniques are NOT applied here.
 *
 * Module 1.7.10 will investigate AQE and skew handling separately.
 *
 * Key questions:
 *
 * 1. Does the skewed dataset produce an imbalanced shuffle?
 * 2. Does one task process substantially more data than others?
 * 3. Does the skewed task take significantly longer?
 * 4. Does memory usage increase for the skewed partition?
 * 5. Does the skewed workload cause spill?
 * 6. Does AQE coalescing solve data skew?
 */
object DataSkewAggregationExperiment {

  private val DefaultTransactionCount = 10000000L
  private val DefaultCustomerCount = 1000000L
  private val DefaultShufflePartitions = 200
  private val DefaultSkewPercentage = 80

  def main(args: Array[String]): Unit = {

    val transactionCount =
      args.headOption
        .map(_.toLong)
        .getOrElse(DefaultTransactionCount)

    val customerCount =
      args.drop(1).headOption
        .map(_.toLong)
        .getOrElse(DefaultCustomerCount)

    val skewPercentage =
      args.drop(2).headOption
        .map(_.toInt)
        .getOrElse(DefaultSkewPercentage)

    val uiPauseSeconds =
      args.find(_.startsWith("--ui-pause="))
        .map(_.stripPrefix("--ui-pause=").toInt)
        .getOrElse(0)

    validateArguments(
      transactionCount,
      customerCount,
      skewPercentage,
      uiPauseSeconds
    )

    val spark =
      EnterpriseSparkSession.create(
        appName = "Module-1.7.9-Data-Skew-Aggregation",
        environment = "dev"
      )

    configureExperiment(spark)

    printExperimentConfiguration(
      transactionCount,
      customerCount,
      skewPercentage
    )

    println()
    println("=" * 100)
    println("RUN 1 — UNIFORM CUSTOMER DISTRIBUTION")
    println("=" * 100)

    val uniformTransactions =
      createUniformTransactions(
        spark,
        transactionCount,
        customerCount
      )

    runAggregationExperiment(
      spark = spark,
      transactions = uniformTransactions,
      runName = "UNIFORM"
    )

    pauseIfRequested(uiPauseSeconds)

    println()
    println("=" * 100)
    println("RUN 2 — SKEWED CUSTOMER DISTRIBUTION")
    println("=" * 100)

    val skewedTransactions =
      createSkewedTransactions(
        spark,
        transactionCount,
        customerCount,
        skewPercentage
      )

    runAggregationExperiment(
      spark = spark,
      transactions = skewedTransactions,
      runName = "SKEWED"
    )

    pauseIfRequested(uiPauseSeconds)

    println()
    println("=" * 100)
    println("EXPERIMENT COMPLETE")
    println("=" * 100)

    spark.stop()
  }

  /**
   * Configure Spark specifically for this experiment.
   *
   * We intentionally keep AQE enabled because the objective is to observe
   * the baseline interaction between skew and normal adaptive execution.
   *
   * Skew-specific optimization is deliberately NOT enabled here.
   */
  private def configureExperiment(
                                   spark: SparkSession
                                 ): Unit = {

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

    /*
     * Important:
     *
     * Do NOT enable:
     *
     * spark.sql.adaptive.skewJoin.enabled
     *
     * That setting is primarily for JOIN skew and is not the focus
     * of this aggregation experiment.
     */
  }

  /**
   * Create approximately uniform customer distribution.
   *
   * Each transaction is deterministically mapped to:
   *
   * customer_id = transaction_id % customerCount
   *
   * With 10M transactions and 1M customers, each customer receives
   * approximately 10 transactions.
   */
  private def createUniformTransactions(
                                         spark: SparkSession,
                                         transactionCount: Long,
                                         customerCount: Long
                                       ): DataFrame = {

    spark.range(0, transactionCount)
      .selectExpr(
        "id AS transaction_id",
        s"id % $customerCount AS customer_id",
        "id % 1000 AS amount"
      )
  }

  /**
   * Create a deliberately skewed customer distribution.
   *
   * Approximately skewPercentage of all transactions are assigned to:
   *
   * customer_id = 0
   *
   * Remaining transactions are distributed across other customers.
   *
   * This creates a classic "hot key".
   */
  private def createSkewedTransactions(
                                        spark: SparkSession,
                                        transactionCount: Long,
                                        customerCount: Long,
                                        skewPercentage: Int
                                      ): DataFrame = {

    val skewThreshold =
      transactionCount * skewPercentage / 100

    spark.range(0, transactionCount)
      .selectExpr(
        "id AS transaction_id",
        s"""
           |CASE
           |  WHEN id < $skewThreshold THEN 0
           |  ELSE ((id - $skewThreshold) % ${customerCount - 1}) + 1
           |END AS customer_id
           |""".stripMargin,
        "id % 1000 AS amount"
      )
  }

  /**
   * Execute the aggregation and print execution evidence.
   */
  private def runAggregationExperiment(
                                        spark: SparkSession,
                                        transactions: DataFrame,
                                        runName: String
                                      ): Unit = {

    println()
    println(s"[$runName] Input partitions:")
    println(transactions.rdd.getNumPartitions)

    println()
    println(s"[$runName] Input distribution sample:")

    transactions
      .groupBy("customer_id")
      .count()
      .orderBy(desc("count"))
      .show(10, truncate = false)

    val aggregation =
      transactions
        .groupBy("customer_id")
        .sum("amount")

    println()
    println(s"[$runName] Initial Physical Plan:")
    aggregation.explain(true)

    println()
    println(s"[$runName] Materializing aggregation...")

    val startTime =
      System.nanoTime()

    aggregation.foreachPartition {
      _: Iterator[Row] =>
        ()
    }

    val durationSeconds =
      (System.nanoTime() - startTime) / 1000000000.0

    println()
    println(
      f"[$runName] Aggregation action duration: $durationSeconds%.3f seconds"
    )

    println()
    println(s"[$runName] Final Executed Physical Plan:")

    val executedPlan =
      aggregation.queryExecution.executedPlan

    println(executedPlan.treeString)

    println()
    println(s"[$runName] Plan Validation:")

    validatePlan(executedPlan.treeString)

    println()
    println(s"[$runName] Aggregation output row count:")

    val outputCount =
      aggregation.count()

    println(
      s"[$runName] Output rows: $outputCount"
    )
  }

  /**
   * Validate that the expected aggregation/shuffle operators exist.
   *
   * This is deliberately simple and human-readable.
   */
  private def validatePlan(
                            plan: String
                          ): Unit = {

    val checks =
      Seq(
        "HashAggregate" -> plan.contains("HashAggregate"),
        "Exchange" -> plan.contains("Exchange"),
        "AQEShuffleRead" -> plan.contains("AQEShuffleRead"),
        "AdaptiveSparkPlan" -> plan.contains("AdaptiveSparkPlan")
      )

    checks.foreach {
      case (name, present) =>
        println(
          f"${name}%-20s : ${if (present) "FOUND" else "NOT FOUND"}"
        )
    }
  }

  /**
   * Print experiment configuration.
   */
  private def printExperimentConfiguration(
                                            transactionCount: Long,
                                            customerCount: Long,
                                            skewPercentage: Int
                                          ): Unit = {

    println()
    println("=" * 100)
    println("MODULE 1.7.9 — DATA SKEW IN AGGREGATION")
    println("=" * 100)

    println(
      s"Transactions              : $transactionCount"
    )

    println(
      s"Customers                 : $customerCount"
    )

    println(
      s"Shuffle partitions        : $DefaultShufflePartitions"
    )

    println(
      "AQE                       : ENABLED"
    )

    println(
      "AQE partition coalescing  : ENABLED"
    )

    println(
      "Skew percentage            : " +
        s"$skewPercentage%"
    )

    println(
      "Skew mitigation            : NOT APPLIED"
    )

    println("=" * 100)
  }


  /**
   * Pause execution so the Spark UI can be inspected.
   *
   * The Spark application remains alive during this pause, which means:
   *
   * http://localhost:4040
   *
   * remains available for inspection.
   *
   * This is especially important for this experiment because we need to inspect:
   *
   *   - Peak Execution Memory
   *   - Spill (Memory)
   *   - Spill (Disk)
   *   - Shuffle Read
   *   - Shuffle Write
   *   - Task duration distribution
   *   - Number of tasks
   *
   * The pause is optional.
   *
   * Example:
   *
   * --ui-pause=60
   *
   * pauses for 60 seconds.
   */
  private def pauseIfRequested(
                                seconds: Int
                              ): Unit = {

    if (seconds > 0) {

      println()
      println("=" * 100)
      println(
        s"UI PAUSE — Spark UI inspection window: $seconds seconds"
      )
      println("=" * 100)

      println()
      println("Open Spark UI:")
      println("http://localhost:4040")

      println()
      println("Inspect the current experiment before execution continues:")
      println("  - SQL tab")
      println("  - Query details")
      println("  - Physical plan")
      println("  - Stage/task metrics")
      println("  - Shuffle Read / Write")
      println("  - Peak Execution Memory")
      println("  - Spill (Memory)")
      println("  - Spill (Disk)")
      println("  - Task duration distribution")

      println()
      println(s"Execution will resume automatically after $seconds seconds...")

      Thread.sleep(seconds * 1000L)

      println()
      println("UI pause completed. Continuing experiment...")
      println("=" * 100)
    }
  }

  /**
   * Validate command-line parameters.
   */
  private def validateArguments(
                                 transactionCount: Long,
                                 customerCount: Long,
                                 skewPercentage: Int,
                                 uiPauseSeconds: Int
                               ): Unit = {

    require(
      transactionCount > 0,
      "transactionCount must be greater than 0"
    )

    require(
      customerCount > 1,
      "customerCount must be greater than 1"
    )

    require(
      skewPercentage >= 50 && skewPercentage < 100,
      "skewPercentage must be between 50 and 99"
    )

    require(
      uiPauseSeconds >= 0,
      "uiPauseSeconds cannot be negative"
    )
  }
}