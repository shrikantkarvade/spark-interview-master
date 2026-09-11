package com.shrikant.spark.aggregations

import com.shrikant.spark.foundation.EnterpriseSparkSession
import org.apache.spark.sql.{Dataset, Row, SparkSession}

import scala.util.Try

/**
 * Module 1.7.6 — AQE Coalescing Experiment
 *
 * Purpose:
 * Investigate how Adaptive Query Execution (AQE) dynamically coalesces
 * shuffle partitions after Spark has runtime information about the
 * actual shuffle data size.
 *
 * Core concept:
 *
 *   Configured shuffle partitions
 *              |
 *              v
 *          Exchange
 *              |
 *              v
 *       Shuffle data
 *              |
 *              v
 *       Runtime statistics
 *              |
 *              v
 *       AQE coalescing
 *              |
 *              v
 *      Fewer downstream tasks
 *
 * Workload:
 *   - 10 million transactions by default
 *   - 1 million distinct customer IDs by default
 *   - SUM(amount) GROUP BY customer_id
 *
 * Experiment:
 *
 * The configured shuffle partition count remains fixed at 200 while AQE
 * coalescing is investigated.
 *
 * Matrix:
 *
 *   AQE enabled
 *   AQE disabled
 *
 * The primary experiment compares:
 *
 *   AQE OFF
 *       vs
 *   AQE ON + partition coalescing
 *
 * Engineering questions:
 *
 *   1. Does AQE change the configured shuffle partition count?
 *   2. How many shuffle partitions are initially requested?
 *   3. How many partitions are actually consumed after AQE?
 *   4. Does AQE reduce the number of downstream tasks?
 *   5. Does AQE reduce overhead caused by many small shuffle partitions?
 *   6. Does coalescing improve execution time for this workload?
 *   7. What happens when the advisory partition size changes?
 *
 * Important experiment-design principle:
 *
 * AQE decisions are made at runtime.
 *
 * Therefore:
 *
 *   queryExecution.sparkPlan
 *
 * represents the initial physical plan, while:
 *
 *   queryExecution.executedPlan
 *
 * can contain an AdaptiveSparkPlan whose final execution has been
 * modified using runtime statistics.
 *
 * The experiment intentionally prints both the initial and final plans
 * so that the difference can be studied.
 *
 * AQE coalescing:
 *
 * spark.sql.shuffle.partitions defines the initial number of shuffle
 * partitions requested by an Exchange.
 *
 * AQE can subsequently combine small adjacent shuffle partitions when
 * the resulting partition sizes are below the configured target.
 *
 * This means:
 *
 *   configured partitions != necessarily final effective partitions
 *
 * Example:
 *
 *   Configured = 200
 *   AQE        = ON
 *
 * Spark may initially create a 200-way shuffle but later consume the
 * shuffle using significantly fewer coalesced partitions.
 *
 * Advisory partition size:
 *
 * spark.sql.adaptive.advisoryPartitionSizeInBytes
 *
 * provides AQE with a target size used when deciding how to combine
 * shuffle partitions.
 *
 * A smaller advisory size generally allows less aggressive coalescing.
 *
 * A larger advisory size generally allows more aggressive coalescing.
 *
 * Important:
 *
 * AQE coalescing should not be confused with skew optimization.
 *
 * Coalescing combines small partitions.
 *
 * Skew handling deals with abnormally large partitions.
 *
 * Skew-specific experiments are covered later in:
 *
 *   Module 1.7.9 — Data Skew in Aggregation
 *   Module 1.7.10 — AQE + Aggregation Skew
 *
 * Execution:
 *
 * ./gradlew runAQECoalescingExperiment
 *
 * Example:
 *
 * ./gradlew runAQECoalescingExperiment \
 * --args="10000000 1000000 --ui-pause=300"
 *
 * Arguments:
 *
 *   1. transactionCount
 *      Number of transactions.
 *
 *   2. customerCount
 *      Number of distinct customers.
 *
 *   3. --ui-pause=<seconds>
 *      Optional pause after execution so that Spark UI can be inspected.
 */
object AQECoalescingExperiment {

  private val DefaultTransactionCount = 10000000L
  private val DefaultCustomerCount = 1000000L

  /**
   * Number of shuffle partitions requested by the experiment.
   *
   * Keeping this fixed allows us to focus on AQE's runtime behavior
   * rather than simultaneously changing the initial shuffle parallelism.
   */
  private val ConfiguredShufflePartitions = 200

  /**
   * AQE advisory partition sizes to investigate later.
   *
   * The initial run uses only the default configured value of 64 MB.
   *
   * These values are retained here as a controlled experiment matrix
   * for subsequent runs if the initial evidence indicates that more
   * investigation is useful.
   */
  private val AdvisoryPartitionSizeMatrix =
    Seq(
      "32MB",
      "64MB",
      "128MB",
      "256MB"
    )

  def main(args: Array[String]): Unit = {

    val transactionCount =
      parseLongArgument(
        args = args,
        argumentIndex = 0,
        defaultValue = DefaultTransactionCount,
        argumentName = "transactionCount"
      )

    val customerCount =
      parseLongArgument(
        args = args,
        argumentIndex = 1,
        defaultValue = DefaultCustomerCount,
        argumentName = "customerCount"
      )

    val uiPauseSeconds =
      parseUiPause(args)

    validateInputs(
      transactionCount = transactionCount,
      customerCount = customerCount,
      uiPauseSeconds = uiPauseSeconds
    )

    /*
     * EnterpriseSparkSession is used instead of creating a SparkSession
     * directly so that this experiment follows the same project-level
     * Spark configuration and conventions as the other modules.
     */
    val spark =
      EnterpriseSparkSession.create(
        "AQE Coalescing Experiment",
        "dev"
      )

    try {

      runExperiment(
        spark = spark,
        transactionCount = transactionCount,
        customerCount = customerCount,
        uiPauseSeconds = uiPauseSeconds
      )

    } finally {

      spark.stop()
    }
  }

  /**
   * Executes the AQE coalescing experiment.
   *
   * The experiment is intentionally divided into two runs:
   *
   *   Run 1 — AQE OFF
   *   Run 2 — AQE ON
   *
   * Both runs use:
   *
   *   - identical input data
   *   - identical aggregation
   *   - identical shuffle partition configuration
   *
   * Only AQE behavior changes.
   *
   * This isolates the effect of Adaptive Query Execution.
   */
  private def runExperiment(
                             spark: SparkSession,
                             transactionCount: Long,
                             customerCount: Long,
                             uiPauseSeconds: Int
                           ): Unit = {

    println()
    println("============================================================")
    println("MODULE 1.7.6 — AQE COALESCING")
    println("============================================================")

    println(s"Transaction count        : $transactionCount")
    println(s"Customer count           : $customerCount")
    println(
      s"Configured shuffle       : $ConfiguredShufflePartitions"
    )

    println(
      s"AQE advisory size       : " +
        s"${spark.conf.get("spark.sql.adaptive.advisoryPartitionSizeInBytes")}"
    )

    println("============================================================")
    println()

    /*
     * ------------------------------------------------------------------------
     * RUN 1 — AQE DISABLED
     * ------------------------------------------------------------------------
     *
     * AQE is disabled so that we can establish the behavior of the original
     * static Spark execution model.
     *
     * With:
     *
     *   spark.sql.shuffle.partitions = 200
     *
     * the aggregation Exchange requests 200 shuffle partitions.
     *
     * There is no AQE stage capable of subsequently coalescing those
     * partitions.
     */
    runSingleExperiment(
      spark = spark,
      transactionCount = transactionCount,
      customerCount = customerCount,
      aqeEnabled = false
    )

    /*
     * ------------------------------------------------------------------------
     * RUN 2 — AQE ENABLED
     * ------------------------------------------------------------------------
     *
     * AQE is enabled together with shuffle partition coalescing.
     *
     * The initial Exchange still uses the configured 200 shuffle partitions.
     *
     * However, after Spark obtains runtime shuffle statistics, AQE may
     * combine multiple small shuffle partitions into fewer downstream
     * partitions.
     */
    runSingleExperiment(
      spark = spark,
      transactionCount = transactionCount,
      customerCount = customerCount,
      aqeEnabled = true
    )

    println()
    println("============================================================")
    println("AQE COALESCING EXPERIMENT COMPLETE")
    println("============================================================")

    println()
    println("Spark UI:")
    println(
      spark.sparkContext.uiWebUrl.getOrElse(
        "Spark UI URL is not available."
      )
    )

    if (uiPauseSeconds > 0) {

      println()
      println(
        s"Spark UI will remain available for $uiPauseSeconds seconds."
      )

      println(
        "Use the Spark SQL tab to inspect both executions."
      )

      println()

      Thread.sleep(
        uiPauseSeconds * 1000L
      )
    }
  }

  /**
   * Executes one controlled AQE experiment.
   *
   * A fresh DataFrame and aggregation are intentionally created for every
   * run.
   *
   * This prevents accidental reuse of a previously constructed query plan
   * when AQE configuration changes between experiments.
   */
  private def runSingleExperiment(
                                   spark: SparkSession,
                                   transactionCount: Long,
                                   customerCount: Long,
                                   aqeEnabled: Boolean
                                 ): Unit = {

    println()
    println("------------------------------------------------------------")

    if (aqeEnabled) {
      println("RUN 2 — AQE ENABLED + COALESCE ENABLED")
    } else {
      println("RUN 1 — AQE DISABLED")
    }

    println("------------------------------------------------------------")

    /*
     * Configure AQE BEFORE constructing the aggregation DataFrame.
     *
     * This makes the experiment deterministic and prevents confusion
     * regarding which query was planned under which configuration.
     */
    spark.conf.set(
      "spark.sql.adaptive.enabled",
      aqeEnabled
    )

    spark.conf.set(
      "spark.sql.adaptive.coalescePartitions.enabled",
      aqeEnabled
    )

    spark.conf.set(
      "spark.sql.shuffle.partitions",
      ConfiguredShufflePartitions
    )

    /*
     * Keep the advisory partition size fixed for the baseline comparison.
     *
     * Additional advisory-size experiments can be introduced later without
     * changing the core AQE ON vs OFF experiment.
     */
    spark.conf.set(
      "spark.sql.adaptive.advisoryPartitionSizeInBytes",
      "64MB"
    )

    println()
    println("Configuration")
    println("------------------------------------------------------------")

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

    println(
      s"spark.sql.shuffle.partitions = " +
        spark.conf.get("spark.sql.shuffle.partitions")
    )

    println(
      s"spark.sql.adaptive.advisoryPartitionSizeInBytes = " +
        spark.conf.get(
          "spark.sql.adaptive.advisoryPartitionSizeInBytes"
        )
    )

    /*
     * ------------------------------------------------------------------------
     * CREATE FRESH INPUT DATAFRAME
     * ------------------------------------------------------------------------
     *
     * The dataset is deterministic.
     *
     * Every experiment therefore processes the same logical workload:
     *
     *   10 million transactions
     *   1 million customers
     *
     * The amount expression is also deterministic.
     */
    val transactions =
      spark.range(0, transactionCount)
        .selectExpr(
          "id AS transaction_id",
          s"id % $customerCount AS customer_id",
          "id % 1000 AS amount"
        )

    println()
    println("Input DataFrame")
    println("------------------------------------------------------------")

    println(
      s"Input partitions = ${transactions.rdd.getNumPartitions}"
    )

    /*
     * ------------------------------------------------------------------------
     * CREATE FRESH AGGREGATION
     * ------------------------------------------------------------------------
     *
     * GROUP BY customer_id requires Spark to establish the required
     * distribution of customer_id values.
     *
     * This normally introduces an Exchange operator.
     */
    val aggregation =
      transactions
        .groupBy("customer_id")
        .sum("amount")

    /*
     * ------------------------------------------------------------------------
     * INITIAL PHYSICAL PLAN
     * ------------------------------------------------------------------------
     *
     * This plan represents the physical plan before runtime AQE decisions
     * have been applied.
     *
     * Pay particular attention to:
     *
     *   Exchange
     *   hashpartitioning(customer_id, 200)
     *
     * The important observation is that AQE does NOT simply replace the
     * configured spark.sql.shuffle.partitions value during initial planning.
     */
    println()
    println("Initial physical plan:")
    println("------------------------------------------------------------")

    aggregation.explain()

    println("------------------------------------------------------------")

    /*
     * ------------------------------------------------------------------------
     * EXECUTE AGGREGATION
     * ------------------------------------------------------------------------
     *
     * foreachPartition forces Spark to execute the entire aggregation while
     * avoiding unnecessary driver-side collection of the result.
     *
     * We intentionally do not call collect().
     */
    val startTimeNanos =
      System.nanoTime()

    aggregation.foreachPartition {
      _: Iterator[Row] =>
        ()
    }

    val elapsedSeconds =
      (System.nanoTime() - startTimeNanos) /
        1000000000.0

    println()
    println(
      f"Application action time: $elapsedSeconds%.3f seconds"
    )

    /*
     * ------------------------------------------------------------------------
     * FINAL EXECUTED PLAN
     * ------------------------------------------------------------------------
     *
     * This is the critical part of the AQE experiment.
     *
     * After execution, Spark has runtime statistics from the shuffle.
     *
     * When AQE is enabled, the executed plan can contain:
     *
     *   AdaptiveSparkPlan
     *       |
     *       +-- ShuffleQueryStage
     *               |
     *               +-- Exchange
     *
     * followed by a coalesced shuffle reader.
     *
     * The final executed plan should therefore be inspected after the
     * action rather than before it.
     */
    val executedPlan =
      aggregation.queryExecution.executedPlan

    println()
    println("Final executed physical plan:")
    println("------------------------------------------------------------")

    println(
      executedPlan.treeString
    )

    println("------------------------------------------------------------")

    /*
     * ------------------------------------------------------------------------
     * PLAN VALIDATION
     * ------------------------------------------------------------------------
     *
     * Validate that the initial Exchange was configured for 200 partitions.
     *
     * When AQE is enabled, the final plan may show a different effective
     * number of partitions downstream of the Exchange.
     *
     * This distinction is fundamental to understanding AQE:
     *
     *   Exchange partition count
     *              !=
     *   final coalesced partition count
     */
    validateExecutedPlan(
      aggregation = aggregation,
      expectedPartitions = ConfiguredShufflePartitions,
      aqeEnabled = aqeEnabled
    )

    /*
     * ------------------------------------------------------------------------
     * RUNTIME INVESTIGATION CHECKLIST
     * ------------------------------------------------------------------------
     *
     * Capture these metrics from the Spark UI.
     */
    println()
    println("Spark UI metrics to capture:")
    println("  • Query duration")
    println("  • Number of stages")
    println("  • Number of tasks")
    println("  • Original shuffle partition count")
    println("  • AQE coalesced partition count")
    println("  • Shuffle bytes written")
    println("  • Shuffle records written")
    println("  • Shuffle read bytes")
    println("  • Shuffle read records")
    println("  • Task duration distribution")
    println("  • Input size / records")
    println("  • Spill memory")
    println("  • Spill disk")
    println("  • Peak execution memory")
    println("  • Shuffle write time")
    println("  • Shuffle read time")
    println("  • Aggregation metrics")
    println("  • SQL adaptive plan")
    println()
  }

  /**
   * Validates the executed physical plan.
   *
   * The aggregation should contain an Exchange using the configured
   * shuffle partition count:
   *
   *   hashpartitioning(customer_id, 200)
   *
   * When AQE is enabled, the executed plan can additionally contain
   * coalesced shuffle-reader behavior.
   *
   * The validation therefore focuses on confirming that the original
   * Exchange was configured correctly rather than incorrectly expecting
   * AQE to rewrite the Exchange itself.
   */
  private def validateExecutedPlan(
                                    aggregation: Dataset[Row],
                                    expectedPartitions: Int,
                                    aqeEnabled: Boolean
                                  ): Unit = {

    val executedPlan =
      aggregation.queryExecution.executedPlan.toString

    /*
     * A GROUP BY requiring redistribution should contain an Exchange.
     */
    if (!executedPlan.contains("Exchange")) {

      throw new IllegalStateException(
        "Physical plan validation failed. " +
          "Expected an Exchange for the aggregation shuffle, " +
          "but no Exchange was found.\n" +
          s"Executed plan:\n$executedPlan"
      )
    }

    /*
     * Verify that the aggregation Exchange was configured with the
     * expected number of shuffle partitions.
     */
    val expectedPartitioningFound =
      executedPlan.contains("hashpartitioning(customer_id") &&
        executedPlan.contains(s", $expectedPartitions)")

    if (!expectedPartitioningFound) {

      throw new IllegalStateException(
        "Physical plan validation failed. " +
          s"Expected hashpartitioning(customer_id, " +
          s"$expectedPartitions), but the executed plan was:\n" +
          executedPlan
      )
    }

    println(
      "PLAN VALIDATION: PASS — " +
        s"Exchange configured for $expectedPartitions partitions."
    )

    /*
     * AQE-specific informational validation.
     *
     * Spark's exact executed-plan representation can vary by Spark version
     * and execution path, so we do not fail the experiment merely because
     * a particular AQE node name is absent.
     *
     * The Spark UI remains the authoritative evidence for the final
     * runtime partition behavior.
     */
    if (aqeEnabled) {

      val adaptivePlanDetected =
        executedPlan.contains("AdaptiveSparkPlan")

      println(
        s"AQE plan detected       : $adaptivePlanDetected"
      )

      val coalescedReaderDetected =
        executedPlan.contains("Coalesced") ||
          executedPlan.contains("AQEShuffleRead")

      println(
        s"AQE shuffle reader node : $coalescedReaderDetected"
      )
    }
  }

  /**
   * Parses a positional Long argument.
   *
   * Missing arguments use the supplied default.
   *
   * Invalid explicitly supplied values fail fast rather than silently
   * changing the experiment workload.
   */
  private def parseLongArgument(
                                 args: Array[String],
                                 argumentIndex: Int,
                                 defaultValue: Long,
                                 argumentName: String
                               ): Long = {

    args
      .lift(argumentIndex)
      .filterNot(_.startsWith("--"))
      .flatMap(
        value =>
          Try(value.toLong).toOption
      )
      .getOrElse {

        args.lift(argumentIndex) match {

          case None =>
            defaultValue

          case Some(value)
            if value.startsWith("--") =>
            defaultValue

          case Some(value) =>
            throw new IllegalArgumentException(
              s"Invalid $argumentName: '$value'. " +
                "Expected a positive integer."
            )
        }
      }
  }

  /**
   * Parses:
   *
   *   --ui-pause=<seconds>
   *
   * The pause keeps the Spark UI available after the experiment so that
   * runtime execution details can be inspected manually.
   */
  private def parseUiPause(
                            args: Array[String]
                          ): Int = {

    args
      .find(
        _.startsWith("--ui-pause=")
      )
      .map { value =>

        val seconds =
          value
            .stripPrefix("--ui-pause=")
            .toInt

        seconds
      }
      .getOrElse(0)
  }

  /**
   * Validates experiment parameters before Spark execution begins.
   */
  private def validateInputs(
                              transactionCount: Long,
                              customerCount: Long,
                              uiPauseSeconds: Int
                            ): Unit = {

    require(
      transactionCount > 0,
      "transactionCount must be greater than zero."
    )

    require(
      customerCount > 0,
      "customerCount must be greater than zero."
    )

    require(
      customerCount <= transactionCount,
      "customerCount must not exceed transactionCount."
    )

    require(
      uiPauseSeconds >= 0,
      "--ui-pause must be zero or greater."
    )
  }
}
