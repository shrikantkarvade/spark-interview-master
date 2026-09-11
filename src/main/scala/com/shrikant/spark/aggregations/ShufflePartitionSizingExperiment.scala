package com.shrikant.spark.aggregations

import com.shrikant.spark.foundation.EnterpriseSparkSession
import org.apache.spark.sql.{Dataset, Row, SparkSession}

import scala.util.Try

/**
 * Module 1.7.5 — Shuffle Partition Sizing Experiment
 *
 * Purpose:
 * Investigate how the configured number of Spark shuffle partitions affects
 * aggregation execution and how Adaptive Query Execution (AQE) subsequently
 * coalesces shuffle partitions at runtime.
 *
 * Workload:
 *   - 10 million transactions by default
 *   - 1 million distinct customer IDs by default
 *   - SUM(amount) GROUP BY customer_id
 *
 * Matrix:
 * 10, 20, 50, 100, 200 shuffle partitions
 *
 * Engineering questions:
 *   1. Does the configured shuffle partition count change the Exchange?
 *      2. How much shuffle data is produced?
 *      3. How many partitions remain after AQE coalescing?
 *      4. Does increasing configured parallelism improve execution?
 *      5. When does increasing shuffle parallelism become unnecessary?
 *
 * Important experiment-design principle:
 *
 * spark.sql.shuffle.partitions influences the Exchange created during
 * physical planning. Therefore the configuration must be set BEFORE the
 * DataFrame used for the aggregation is created/executed for each matrix
 * entry.
 *
 * A fresh source DataFrame and fresh aggregation DataFrame are intentionally
 * created inside every matrix iteration. This prevents accidental reuse of
 * a previously planned query during the controlled experiment.
 *
 * AQE:
 *
 * AQE is intentionally left enabled because Module 1.7.5 is designed to
 * demonstrate the distinction between:
 *
 * configured shuffle partitions
 * versus
 * final AQE coalesced partitions
 *
 * For example, a query configured with 200 shuffle partitions may ultimately
 * execute with significantly fewer partitions when AQE determines that the
 * shuffle data is small enough to combine.
 *
 * Execution:
 *
 * ./gradlew runShufflePartitionSizingExperiment
 *
 * Example:
 *
 * ./gradlew runShufflePartitionSizingExperiment \
 * --args="10000000 1000000 --ui-pause=300"
 *
 * Arguments:
 *
 *   1. transactionCount
 *      2. customerCount
 *      3. --ui-pause=<seconds>
 */
object ShufflePartitionSizingExperiment {

  private val DefaultTransactionCount = 10000000L
  private val DefaultCustomerCount = 1000000L

  /**
   * Controlled shuffle partition matrix.
   *
   * The workload remains constant while only the configured shuffle
   * partition count changes.
   */
  private val ShufflePartitionMatrix =
    Seq(10, 20, 50, 100, 200)

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

    val spark =
      EnterpriseSparkSession.create(
        "Shuffle Partition Sizing Experiment",
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
   * Executes the complete shuffle partition sizing matrix.
   *
   * Each matrix iteration:
   *
   *   1. Configures spark.sql.shuffle.partitions.
   *      2. Creates a fresh source DataFrame.
   *      3. Creates a fresh aggregation DataFrame.
   *      4. Prints the initial physical plan.
   *      5. Executes the aggregation.
   *      6. Prints the final AQE plan.
   *      7. Validates the actual Exchange partition count.
   *      8. Leaves the Spark UI available for runtime analysis.
   */
  private def runExperiment(
                             spark: SparkSession,
                             transactionCount: Long,
                             customerCount: Long,
                             uiPauseSeconds: Int
                           ): Unit = {

    println()
    println("============================================================")
    println("MODULE 1.7.5 — SHUFFLE PARTITION SIZING")
    println("============================================================")
    println(s"Transaction count : $transactionCount")
    println(s"Customer count    : $customerCount")
    println(
      s"Partition matrix  : ${ShufflePartitionMatrix.mkString(", ")}"
    )
    println(
      s"AQE enabled       : ${spark.conf.get("spark.sql.adaptive.enabled")}"
    )
    println("============================================================")
    println()

    /*
     * Do not create the source DataFrame here.
     *
     * Although the source itself does not contain the Exchange, creating a
     * fresh source for every matrix iteration gives us maximum isolation
     * between the experiments and makes the benchmark easier to reason about.
     */
    ShufflePartitionMatrix.zipWithIndex.foreach {
      case (configuredShufflePartitions, matrixIndex) =>

        println()
        println("------------------------------------------------------------")
        println(
          s"QUERY $matrixIndex — " +
            s"$configuredShufflePartitions SHUFFLE PARTITIONS"
        )
        println("------------------------------------------------------------")

        /*
         * Configure the shuffle partition count BEFORE creating the
         * aggregation DataFrame.
         *
         * This configuration controls the number of partitions requested
         * by the Exchange generated for the aggregation shuffle.
         */
        spark.conf.set(
          "spark.sql.shuffle.partitions",
          configuredShufflePartitions
        )

        val actualConfiguredPartitions =
          spark.conf
            .get("spark.sql.shuffle.partitions")
            .toInt

        println(
          s"spark.sql.shuffle.partitions = $actualConfiguredPartitions"
        )

        /*
         * Fresh source DataFrame for this experiment.
         *
         * The data is deterministic, so every matrix entry processes the same
         * logical workload.
         */
        val transactions =
          spark.range(0, transactionCount)
            .selectExpr(
              "id AS transaction_id",
              s"id % $customerCount AS customer_id",
              "id % 1000 AS amount"
            )

        /*
         * Fresh aggregation DataFrame.
         *
         * The aggregation requires a shuffle because customer_id is not
         * guaranteed to be partitioned according to the required grouping
         * distribution.
         */
        val aggregation =
          transactions
            .groupBy("customer_id")
            .sum("amount")

        println()
        println("Initial physical plan:")
        println("------------------------------------------------------------")
        aggregation.explain()
        println("------------------------------------------------------------")

        /*
         * Execute the complete aggregation without collecting the result
         * to the driver.
         *
         * foreachPartition forces Spark to materialize the aggregation while
         * avoiding unnecessary driver-side result materialization.
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
         * AQE may modify the execution plan after runtime statistics become
         * available.
         *
         * Therefore the executed plan is inspected only after the action.
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
         * Validate the Exchange actually used by the executed query.
         *
         * This validation happens AFTER execution because AQE wraps and
         * transforms the physical execution plan.
         *
         * We validate the configured Exchange separately from the final
         * AQE read behavior.
         */
        validateExecutedPlan(
          aggregation = aggregation,
          expectedPartitions = configuredShufflePartitions
        )

        println()
        println("Spark UI metrics to capture for this query:")
        println("  • Query duration")
        println("  • Original shuffle partitions")
        println("  • AQE coalesced partitions")
        println("  • Shuffle records written")
        println("  • Shuffle bytes written")
        println("  • AQE partition data size")
        println("  • Partial aggregation output rows")
        println("  • Final aggregation output rows")
        println("  • Peak aggregation memory")
        println("  • Spill size")
        println("  • Sort fallback")
        println("  • Shuffle write/read time")
        println("  • Whole-stage codegen time")
        println()
    }

    println()
    println("============================================================")
    println("MATRIX EXECUTION COMPLETE")
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
        "Use the Spark SQL tab to inspect each matrix query."
      )
      println()

      Thread.sleep(
        uiPauseSeconds * 1000L
      )
    }
  }

  /**
   * Validates the executed physical plan.
   *
   * Spark's executed plan may contain:
   *
   * AdaptiveSparkPlan
   * -> ShuffleQueryStage
   * -> Exchange
   *
   * Therefore validation is performed after execution rather than relying
   * on queryExecution.sparkPlan before Spark has inserted the required
   * distribution Exchange.
   *
   * The Exchange should contain:
   *
   * hashpartitioning(customer_id, <expectedPartitions>)
   */
  private def validateExecutedPlan(
                                    aggregation: Dataset[Row],
                                    expectedPartitions: Int
                                  ): Unit = {

    val executedPlan =
      aggregation.queryExecution.executedPlan.toString

    if (!executedPlan.contains("Exchange")) {
      throw new IllegalStateException(
        "Physical plan validation failed. " +
          s"Expected an Exchange operator for " +
          s"$expectedPartitions shuffle partitions, " +
          "but no Exchange was found in the executed plan.\n" +
          s"Executed plan:\n$executedPlan"
      )
    }

    val expectedPartitioningFound =
      executedPlan.contains("hashpartitioning(customer_id") &&
        executedPlan.contains(
          s", $expectedPartitions)"
        )

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
        s"Exchange used $expectedPartitions shuffle partitions."
    )
  }

  /**
   * Parses a positional Long argument.
   *
   * Missing arguments use the supplied default.
   * Invalid explicitly supplied values fail fast.
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
      .flatMap(value => Try(value.toLong).toOption)
      .getOrElse {

        args.lift(argumentIndex) match {

          case None =>
            defaultValue

          case Some(value) if value.startsWith("--") =>
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
   * --ui-pause=<seconds>
   *
   * The pause is useful when running locally because it keeps the Spark UI
   * alive long enough to inspect the SQL execution details.
   */
  private def parseUiPause(
                            args: Array[String]
                          ): Int = {

    args
      .find(_.startsWith("--ui-pause="))
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