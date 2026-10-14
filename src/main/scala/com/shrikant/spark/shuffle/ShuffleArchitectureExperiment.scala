package com.shrikant.spark.shuffle

import com.shrikant.spark.foundation.EnterpriseSparkSession
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * Module 1.8.1
 *
 * Shuffle Architecture & Data Movement
 *
 * Objective:
 *   Understand what Spark physically does when a workload requires data
 *   to move between partitions.
 *
 * This experiment intentionally keeps the business operation simple:
 *
 *   Input Dataset
 *        |
 *        v
 *   groupBy(customerId)
 *        |
 *        v
 *      Exchange
 *   (hash partitioning)
 *        |
 *        v
 *   Partial/Final Aggregation
 *
 * The objective is NOT to optimize the workload yet.
 *
 * We want to establish a baseline understanding of:
 *
 *   1. Why Spark creates an Exchange.
 *   2. Why the Exchange represents a shuffle boundary.
 *   3. How input partitions relate to shuffle map tasks.
 *   4. How shuffle partitions relate to downstream tasks.
 *   5. How shuffle write/read metrics appear in Spark UI.
 *   6. How the physical plan maps to Jobs, Stages and Tasks.
 *
 * Later Module 1.8 experiments will deliberately change one variable at
 * a time, such as partition count, spill pressure, compression, etc.
 */
object ShuffleArchitectureExperiment {

  // ---------------------------------------------------------------------------
  // Configuration model
  // ---------------------------------------------------------------------------

  final case class Config(
                           rowCount: Long,
                           customerCount: Long,
                           inputPartitions: Int,
                           shufflePartitions: Int,
                           uiPauseSeconds: Int
                         )

  // ---------------------------------------------------------------------------
  // Main entry point
  // ---------------------------------------------------------------------------

  def main(args: Array[String]): Unit = {

    val config = parseArguments(args)

    println()
    println("=" * 90)
    println("MODULE 1.8.1 - SHUFFLE ARCHITECTURE & DATA MOVEMENT")
    println("=" * 90)

    println()
    println("Experiment configuration")
    println("-" * 90)
    println(s"Rows              : ${config.rowCount}")
    println(s"Customers         : ${config.customerCount}")
    println(s"Input partitions  : ${config.inputPartitions}")
    println(s"Shuffle partitions: ${config.shufflePartitions}")
    println(s"UI pause seconds   : ${config.uiPauseSeconds}")
    println()

    // -------------------------------------------------------------------------
    // EnterpriseSparkSession
    //
    // We intentionally use the repository's standard SparkSession factory
    // rather than constructing SparkSession independently in every experiment.
    //
    // This keeps application/session configuration centralized and makes the
    // experiment behave consistently with the rest of the repository.
    // -------------------------------------------------------------------------

    val spark = EnterpriseSparkSession.create(
      appName = "Module-1.8.1-Shuffle-Architecture",
      environment = "dev"
    )

    try {

      // -----------------------------------------------------------------------
      // Explicitly configure shuffle partitions for this experiment.
      //
      // spark.sql.shuffle.partitions controls the number of partitions created
      // by many shuffle-based SQL/DataFrame operations.
      //
      // Important:
      //
      // inputPartitions != shufflePartitions
      //
      // Input partitions describe how the source data is initially divided.
      //
      // Shuffle partitions describe how Spark redistributes data after the
      // Exchange.
      // -----------------------------------------------------------------------

      spark.conf.set(
        "spark.sql.shuffle.partitions",
        config.shufflePartitions
      )

      println("Spark configuration")
      println("-" * 90)
      println(
        s"spark.sql.shuffle.partitions = " +
          spark.conf.get("spark.sql.shuffle.partitions")
      )

      println()
      println("Spark version: " + spark.version)

      // -----------------------------------------------------------------------
      // Create deterministic input data.
      //
      // Deterministic data is important for benchmarking because every run
      // should operate on the same logical workload.
      //
      // customerId is intentionally generated using modulo customerCount.
      //
      // Therefore:
      //
      //   rowCount     = total input records
      //   customerCount = number of distinct grouping keys
      //
      // This also gives us control over aggregation cardinality.
      // -----------------------------------------------------------------------

      val input = createInputData(
        spark = spark,
        rowCount = config.rowCount,
        customerCount = config.customerCount,
        inputPartitions = config.inputPartitions
      )

      println()
      println("Input dataset")
      println("-" * 90)
      println(s"Input partitions: ${input.rdd.getNumPartitions}")

      // -----------------------------------------------------------------------
      // Materialize the input.
      //
      // This separates input-data generation from the actual shuffle workload.
      // Without materialization, Spark could combine generation and the query
      // into the same execution lineage.
      // -----------------------------------------------------------------------

      val materializationStart = System.nanoTime()

      val inputCount = input.count()

      val materializationSeconds =
        (System.nanoTime() - materializationStart) / 1e9

      println(s"Input rows      : $inputCount")
      println(f"Materialization : $materializationSeconds%.3f seconds")

      // -----------------------------------------------------------------------
      // Experiment
      //
      // groupBy(customerId) requires all records belonging to the same
      // customer to reach the same downstream partition.
      //
      // Spark therefore introduces an Exchange.
      //
      // Conceptually:
      //
      //   Input
      //      |
      //      v
      //   Partial Aggregate
      //      |
      //      v
      //   Exchange
      //   hashpartitioning(customerId)
      //      |
      //      v
      //   Final Aggregate
      //
      // The Exchange is the critical point we want to observe.
      // -----------------------------------------------------------------------

      println()
      println("=" * 90)
      println("EXPERIMENT: GROUP BY CUSTOMER")
      println("=" * 90)

      val result: DataFrame =
        input
          .groupBy("customerId")
          .count()

      // -----------------------------------------------------------------------
      // Inspect the complete query plan BEFORE execution.
      //
      // explain(true) exposes:
      //
      //   Parsed Logical Plan
      //   Analyzed Logical Plan
      //   Optimized Logical Plan
      //   Physical Plan
      //
      // The physical plan is especially important because it shows the actual
      // execution operators Spark intends to use.
      // -----------------------------------------------------------------------

      println()
      println("Query plan - explain(true)")
      println("-" * 90)

      result.explain(true)

      // -----------------------------------------------------------------------
      // Inspect the physical plan programmatically.
      //
      // We use the executed plan after the action because this is the plan
      // Spark actually executed.
      // -----------------------------------------------------------------------

      val executedPlanBeforeAction =
        result.queryExecution.executedPlan

      println()
      println("Executed physical plan before action")
      println("-" * 90)
      println(executedPlanBeforeAction)

      // -----------------------------------------------------------------------
      // Execute the query.
      //
      // count() is deliberately used as the action because we need Spark to
      // materialize the complete aggregation result.
      //
      // The action causes the shuffle to actually occur.
      // -----------------------------------------------------------------------

      val queryStart = System.nanoTime()

      val resultCount = result.count()

      val querySeconds =
        (System.nanoTime() - queryStart) / 1e9

      println()
      println("Execution result")
      println("-" * 90)
      println(s"Distinct customers returned: $resultCount")
      println(f"Query execution time       : $querySeconds%.3f seconds")

      // -----------------------------------------------------------------------
      // Inspect the executed plan again after execution.
      //
      // This is useful when Adaptive Query Execution is enabled because AQE
      // can modify parts of the physical execution plan at runtime.
      // -----------------------------------------------------------------------

      val executedPlanAfterAction =
        result.queryExecution.executedPlan

      println()
      println("Executed physical plan after action")
      println("-" * 90)
      println(executedPlanAfterAction)

      // -----------------------------------------------------------------------
      // Basic validation
      //
      // The generated dataset contains customer IDs in the range:
      //
      //   0 ... customerCount - 1
      //
      // Therefore the number of grouped records should equal customerCount
      // when rowCount >= customerCount.
      //
      // We validate the result rather than treating execution time alone as
      // evidence that the experiment succeeded.
      // -----------------------------------------------------------------------

      if (resultCount != config.customerCount) {
        throw new IllegalStateException(
          s"Unexpected aggregation result. " +
            s"Expected ${config.customerCount}, got $resultCount."
        )
      }

      println()
      println("Validation")
      println("-" * 90)
      println("PASS - aggregation result matches expected customer cardinality.")

      // -----------------------------------------------------------------------
      // Spark UI pause
      //
      // IMPORTANT:
      //
      // spark.stop() must NOT happen before this pause.
      //
      // Spark UI is normally available at:
      //
      //   http://localhost:4040
      //
      // During the pause you can inspect:
      //
      //   Jobs
      //   Stages
      //   Tasks
      //   SQL
      //   Shuffle Read
      //   Shuffle Write
      //   Records Read
      //   Records Written
      //   Task duration
      //   GC time
      //   Spill metrics
      //
      // This gives us evidence for the documentation rather than relying
      // solely on console timing.
      // -----------------------------------------------------------------------

      pauseIfRequested(config.uiPauseSeconds)

    } finally {

      // -----------------------------------------------------------------------
      // Stop Spark only AFTER the optional UI pause.
      //
      // This is critical because stopping Spark terminates the SparkContext
      // and therefore removes the local Spark UI.
      // -----------------------------------------------------------------------

      spark.stop()

      println()
      println("=" * 90)
      println("MODULE 1.8.1 COMPLETE")
      println("=" * 90)
    }
  }

  // ===========================================================================
  // Input data generation
  // ===========================================================================

  /**
   * Creates deterministic benchmark data.
   *
   * Repartition is used here intentionally so that we can explicitly control
   * the number of input partitions.
   *
   * The input partition count is independent from the shuffle partition count.
   */
  private def createInputData(
                               spark: SparkSession,
                               rowCount: Long,
                               customerCount: Long,
                               inputPartitions: Int
                             ): DataFrame = {

    import spark.implicits._

    spark.range(
        start = 0L,
        end = rowCount,
        step = 1L,
        numPartitions = inputPartitions
      )
      .selectExpr(
        s"id % $customerCount AS customerId",
        "id AS transactionId",
        "id * 10 AS amount"
      )
  }

  // ===========================================================================
  // CLI argument parsing
  // ===========================================================================

  /**
   * Expected arguments:
   *
   *   rowCount customerCount inputPartitions shufflePartitions
   *
   * Optional:
   *
   *   --ui-pause=60
   *
   * Example:
   *
   *   ./gradlew runShuffleArchitectureExperiment \
   *     --args="10000000 1000000 6 64 --ui-pause=120"
   */
  private def parseArguments(args: Array[String]): Config = {

    val positionalArgs =
      args.filterNot(_.startsWith("--"))

    if (positionalArgs.length != 4) {
      printUsageAndExit()
    }

    val rowCount =
      parseLong(
        positionalArgs(0),
        "rowCount"
      )

    val customerCount =
      parseLong(
        positionalArgs(1),
        "customerCount"
      )

    val inputPartitions =
      parseInt(
        positionalArgs(2),
        "inputPartitions"
      )

    val shufflePartitions =
      parseInt(
        positionalArgs(3),
        "shufflePartitions"
      )

    val uiPauseSeconds =
      args
        .find(_.startsWith("--ui-pause="))
        .map(_.stripPrefix("--ui-pause=").trim)
        .map(value => parseInt(value, "uiPauseSeconds"))
        .getOrElse(0)

    if (rowCount <= 0) {
      throw new IllegalArgumentException(
        "rowCount must be greater than zero."
      )
    }

    if (customerCount <= 0) {
      throw new IllegalArgumentException(
        "customerCount must be greater than zero."
      )
    }

    if (inputPartitions <= 0) {
      throw new IllegalArgumentException(
        "inputPartitions must be greater than zero."
      )
    }

    if (shufflePartitions <= 0) {
      throw new IllegalArgumentException(
        "shufflePartitions must be greater than zero."
      )
    }

    if (uiPauseSeconds < 0) {
      throw new IllegalArgumentException(
        "uiPauseSeconds cannot be negative."
      )
    }

    if (customerCount > rowCount) {
      throw new IllegalArgumentException(
        "customerCount cannot be greater than rowCount for this experiment."
      )
    }

    Config(
      rowCount = rowCount,
      customerCount = customerCount,
      inputPartitions = inputPartitions,
      shufflePartitions = shufflePartitions,
      uiPauseSeconds = uiPauseSeconds
    )
  }

  // ===========================================================================
  // Parsing helpers
  // ===========================================================================

  private def parseLong(
                         value: String,
                         argumentName: String
                       ): Long = {

    try {
      value.toLong
    } catch {
      case _: NumberFormatException =>
        throw new IllegalArgumentException(
          s"$argumentName must be a valid Long. Received: '$value'."
        )
    }
  }

  private def parseInt(
                        value: String,
                        argumentName: String
                      ): Int = {

    try {
      value.toInt
    } catch {
      case _: NumberFormatException =>
        throw new IllegalArgumentException(
          s"$argumentName must be a valid Int. Received: '$value'."
        )
    }
  }

  // ===========================================================================
  // Spark UI pause
  // ===========================================================================

  /**
   * Pauses execution so the developer can inspect Spark UI.
   *
   * We deliberately use Thread.sleep rather than StdIn.readLine().
   *
   * This makes the pause deterministic and works reliably when Gradle launches
   * the JavaExec process.
   */
  private def pauseIfRequested(
                                uiPauseSeconds: Int
                              ): Unit = {

    if (uiPauseSeconds > 0) {

      println()
      println("=" * 90)
      println(s"Spark UI pause enabled: $uiPauseSeconds seconds")
      println("Open Spark UI: http://localhost:4040")
      println()
      println("Recommended inspection:")
      println("  1. SQL")
      println("  2. Jobs")
      println("  3. Stages")
      println("  4. Tasks")
      println("  5. Shuffle Read / Shuffle Write")
      println("  6. Records Read / Records Written")
      println("  7. Spill metrics")
      println("  8. GC time")
      println("=" * 90)

      Thread.sleep(uiPauseSeconds.toLong * 1000L)
    }
  }

  // ===========================================================================
  // Usage
  // ===========================================================================

  private def printUsageAndExit(): Nothing = {

    println()
    println("Usage:")
    println(
      "./gradlew runShuffleArchitectureExperiment " +
        "--args=\"<rowCount> <customerCount> <inputPartitions> " +
        "<shufflePartitions> [--ui-pause=<seconds>]\""
    )

    println()
    println("Example:")
    println(
      "./gradlew runShuffleArchitectureExperiment " +
        "--args=\"10000000 1000000 6 64 --ui-pause=120\""
    )

    throw new IllegalArgumentException(
      "Invalid arguments."
    )
  }
}