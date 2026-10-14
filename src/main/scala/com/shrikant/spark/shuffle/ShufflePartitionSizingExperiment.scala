package com.shrikant.spark.shuffle

import com.shrikant.spark.foundation.EnterpriseSparkSession
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable.ArrayBuffer

/**
* Module 1.8.2 - Shuffle Partition Sizing Experiment
  *
* Objective:
  *
* Understand how the configured number of shuffle partitions affects
  * Spark aggregation execution.
*
* Controlled variables:
  *
*   - Input row count
*   - Customer cardinality
  *   - Input partition count
*   - Aggregation logic
  *   - AQE configuration
  *
* Independent variable:
  *
*   - spark.sql.shuffle.partitions
*
* Default experiment matrix:
  *
*   8
*   16
*   32
*   64
*   128
*   256
*
* Example - complete matrix:
  *
*   ./gradlew runShufflePartitionSizingExperiment \
  *     --args="10000000 1000000 6 matrix"
*
* Example - single configuration:
  *
*   ./gradlew runShufflePartitionSizingExperiment \
  *     --args="10000000 1000000 6 64"
*
* Example - single configuration with Spark UI pause:
  *
*   ./gradlew runShufflePartitionSizingExperiment \
  *     --args="10000000 1000000 6 64 --ui-pause=120"
*
* Example - custom matrix:
  *
*   ./gradlew runShufflePartitionSizingExperiment \
  *     --args="10000000 1000000 6 matrix=8,16,32,64,128,256"
*
* Spark UI:
  *
*   http://localhost:4040
*
* Production engineering questions:
  *
*   Too few partitions:
  *
*     - Larger shuffle partitions
*     - Larger tasks
  *     - Higher memory pressure
*     - Increased spill risk
*     - Potential stragglers
  *
*   Too many partitions:
  *
*     - Smaller shuffle partitions
*     - More tasks
  *     - More scheduler overhead
*     - More task-management overhead
  *
*   AQE:
  *
*     - Can inspect actual shuffle statistics
*     - Can coalesce small shuffle partitions
*     - Therefore configured shuffle partitions and final runtime
*       partitions can be different.
  */
object ShufflePartitionSizingExperiment {

  // ---------------------------------------------------------------------------
  // Result model
  // ---------------------------------------------------------------------------
  //
  // We store the result of every configuration so that the application can
  // print one consolidated comparison table after the matrix completes.
  // ---------------------------------------------------------------------------

  final case class ExperimentResult(
                                     shufflePartitions: Int,
                                     durationMs: Long,
                                     resultRowCount: Long
                                   )

  // ---------------------------------------------------------------------------
  // Main
  // ---------------------------------------------------------------------------

  def main(args: Array[String]): Unit = {

    if (args.length < 4) {
      printUsage()
      sys.error("Invalid arguments.")
    }

    // -------------------------------------------------------------------------
    // Parse and validate the common benchmark parameters.
    // -------------------------------------------------------------------------

    val rowCount =
      parsePositiveLong(args(0), "rowCount")

    val customerCount =
      parsePositiveLong(args(1), "customerCount")

    val inputPartitions =
      parsePositiveInt(args(2), "inputPartitions")

    // -------------------------------------------------------------------------
    // Argument four can be:
    //
    //   64
    //
    // or:
    //
    //   matrix
    //
    // or:
    //
    //   matrix=8,16,32,64,128,256
    // -------------------------------------------------------------------------

    val experimentSpec = args(3)

    val uiPauseSeconds =
      extractUiPause(args.drop(4))

    val shufflePartitionValues =
      parseExperimentSpec(experimentSpec)

    validateShufflePartitions(shufflePartitionValues)

    // -------------------------------------------------------------------------
    // Create the repository-standard Enterprise SparkSession.
    // -------------------------------------------------------------------------

    val spark =
      EnterpriseSparkSession.create(
        appName = "Module-1.8.2-Shuffle-Partition-Sizing",
        environment = "dev"
      )

    try {

      printExperimentConfiguration(
        spark = spark,
        rowCount = rowCount,
        customerCount = customerCount,
        inputPartitions = inputPartitions,
        shufflePartitionValues = shufflePartitionValues,
        uiPauseSeconds = uiPauseSeconds
      )

      // -----------------------------------------------------------------------
      // Create the deterministic source dataset once.
      //
      // We intentionally DO NOT execute input.count().
      //
      // Why?
      //
      // Module 1.8.2 is measuring the effect of shuffle partition sizing.
      // Running an additional action before every experiment would introduce
      // unrelated execution overhead and additional Spark jobs.
      // -----------------------------------------------------------------------

      val input =
        createInputData(
          spark = spark,
          rowCount = rowCount,
          customerCount = customerCount,
          inputPartitions = inputPartitions
        )

      printInputDescription(
        rowCount = rowCount,
        customerCount = customerCount,
        inputPartitions = inputPartitions
      )

      // -----------------------------------------------------------------------
      // Execute each configured shuffle-partition value.
      // -----------------------------------------------------------------------

      val results =
        ArrayBuffer.empty[ExperimentResult]

      shufflePartitionValues.foreach { partitionCount =>

        val experimentResult =
          runSingleExperiment(
            spark = spark,
            input = input,
            rowCount = rowCount,
            customerCount = customerCount,
            inputPartitions = inputPartitions,
            shufflePartitions = partitionCount,
            uiPauseSeconds = uiPauseSeconds
          )

        results += experimentResult
      }

      // -----------------------------------------------------------------------
      // Print the final benchmark matrix.
      // -----------------------------------------------------------------------

      printSummary(results.toSeq)

      printProductionGuidance()

      // -----------------------------------------------------------------------
      // If UI pause was requested in matrix mode, keep Spark alive after the
      // complete matrix so that the final query can still be inspected.
      //
      // For detailed UI screenshots of individual configurations, single-run
      // mode is preferred.
      // -----------------------------------------------------------------------

      if (uiPauseSeconds > 0 && shufflePartitionValues.size > 1) {

        println()
        println("=" * 90)
        println(s"FINAL MATRIX UI PAUSE: $uiPauseSeconds seconds")
        println("=" * 90)
        println()
        println("Open Spark UI:")
        println("http://localhost:4040")
        println()
        println("The latest experiment remains available in the SQL tab.")
        println()
        println("Press ENTER to stop early, or wait for the timeout.")
        println()

        pauseWithUi(uiPauseSeconds)
      }

    } finally {

      println()
      println("Stopping Spark...")

      spark.stop()

      println("Spark stopped.")
    }
  }

  // ===========================================================================
  // INPUT DATASET
  // ===========================================================================

  /**
   * Creates the deterministic benchmark dataset.
   *
   * customerId:
   *
   *   id % customerCount
   *
   * This gives us a controlled key-cardinality workload.
   */
  private def createInputData(
                               spark: SparkSession,
                               rowCount: Long,
                               customerCount: Long,
                               inputPartitions: Int
                             ): DataFrame = {

    spark
      .range(
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
  // SINGLE EXPERIMENT
  // ===========================================================================

  /**
   * Runs one shuffle-partition configuration.
   *
   * Only spark.sql.shuffle.partitions changes between experiments.
   */
  private def runSingleExperiment(
                                   spark: SparkSession,
                                   input: DataFrame,
                                   rowCount: Long,
                                   customerCount: Long,
                                   inputPartitions: Int,
                                   shufflePartitions: Int,
                                   uiPauseSeconds: Int
                                 ): ExperimentResult = {

    println()
    println()
    println("=" * 90)
    println(s"EXPERIMENT: shuffle partitions = $shufflePartitions")
    println("=" * 90)

    println(s"Input rows         : $rowCount")
    println(s"Customer count     : $customerCount")
    println(s"Input partitions   : $inputPartitions")
    println(s"Shuffle partitions : $shufflePartitions")
    println(
      s"AQE enabled        : ${spark.conf.get("spark.sql.adaptive.enabled")}"
    )

    // -------------------------------------------------------------------------
    // This is the independent variable for this experiment.
    // -------------------------------------------------------------------------

    spark.conf.set(
      "spark.sql.shuffle.partitions",
      shufflePartitions.toString
    )

    // -------------------------------------------------------------------------
    // Build the same aggregation for every experiment.
    //
    // Expected conceptual execution:
    //
    //   Range
    //      |
    //   Project
    //      |
    //   Local HashAggregate
    //      |
    //   Exchange
    //      |
    //   Final HashAggregate
    // -------------------------------------------------------------------------

    val result =
      input
        .groupBy("customerId")
        .count()

    // -------------------------------------------------------------------------
    // Inspect the initial plan before execution.
    //
    // We expect to see:
    //
    //   hashpartitioning(customerId, N)
    //
    // where N is the current configured shuffle partition count.
    // -------------------------------------------------------------------------

    println()
    println("----- INITIAL / EXPLAIN PLAN -----")
    println()

    result.explain(true)

    // -------------------------------------------------------------------------
    // Execute the aggregation.
    //
    // IMPORTANT:
    //
    // result.count() returns the number of grouped rows.
    //
    // Since the deterministic dataset has customerCount distinct customer IDs,
    // the expected value is customerCount.
    //
    // The Spark SQL physical plan may additionally contain a final
    // SinglePartition exchange because count() itself is a global aggregation.
    // -------------------------------------------------------------------------

    val startNs =
      System.nanoTime()

    val resultRowCount =
      result.count()

    val durationMs =
      (System.nanoTime() - startNs) / 1000000L

    // -------------------------------------------------------------------------
    // Validate the result.
    // -------------------------------------------------------------------------

    if (resultRowCount != customerCount) {

      sys.error(
        s"""
           |Validation failed.
           |
           |Expected grouped rows : $customerCount
           |Actual grouped rows   : $resultRowCount
           |Shuffle partitions    : $shufflePartitions
           |""".stripMargin
      )
    }

    // -------------------------------------------------------------------------
    // Inspect the executed adaptive plan.
    //
    // This is critical for Module 1.8.2 because AQE may change the runtime
    // partitioning after the shuffle statistics become available.
    // -------------------------------------------------------------------------

    println()
    println("----- EXECUTED ADAPTIVE PLAN -----")
    println()

    println(result.queryExecution.executedPlan)

    // -------------------------------------------------------------------------
    // Print this experiment's measured result.
    // -------------------------------------------------------------------------

    println()
    println("-" * 90)
    println(s"Shuffle partitions : $shufflePartitions")
    println(s"Execution time     : $durationMs ms")
    println(s"Grouped rows       : $resultRowCount")
    println("-" * 90)

    // -------------------------------------------------------------------------
    // Optional Spark UI pause.
    //
    // During this pause:
    //
    //   http://localhost:4040
    //
    // can be used to inspect:
    //
    //   - SQL query
    //   - Exchange
    //   - Shuffle write
    //   - Shuffle read
    //   - Partition count
    //   - AQE coalescing
    //   - Task duration
    //   - Spill
    //   - Peak memory
    // -------------------------------------------------------------------------

    if (uiPauseSeconds > 0) {

      println()
      println("=" * 90)
      println(s"UI PAUSE: $uiPauseSeconds seconds")
      println("=" * 90)
      println()
      println("Open Spark UI:")
      println("http://localhost:4040")
      println()
      println("Inspect the latest SQL query.")
      println()
      println("Press ENTER to stop early, or wait for the timeout.")
      println()

      pauseWithUi(uiPauseSeconds)
    }

    ExperimentResult(
      shufflePartitions = shufflePartitions,
      durationMs = durationMs,
      resultRowCount = resultRowCount
    )
  }

  // ===========================================================================
  // CONFIGURATION OUTPUT
  // ===========================================================================

  private def printExperimentConfiguration(
                                            spark: SparkSession,
                                            rowCount: Long,
                                            customerCount: Long,
                                            inputPartitions: Int,
                                            shufflePartitionValues: Seq[Int],
                                            uiPauseSeconds: Int
                                          ): Unit = {

    println()
    println("=" * 90)
    println("MODULE 1.8.2 - SHUFFLE PARTITION SIZING")
    println("=" * 90)

    println(s"Row count          : $rowCount")
    println(s"Customer count     : $customerCount")
    println(s"Input partitions   : $inputPartitions")
    println(
      s"Shuffle partitions : ${shufflePartitionValues.mkString(", ")}"
    )
    println(
      s"AQE enabled        : ${spark.conf.get("spark.sql.adaptive.enabled")}"
    )
    println(s"UI pause seconds   : $uiPauseSeconds")

    println("=" * 90)
    println()
  }

  private def printInputDescription(
                                     rowCount: Long,
                                     customerCount: Long,
                                     inputPartitions: Int
                                   ): Unit = {

    println("Input Dataset")
    println("-" * 90)

    println(s"Rows             : $rowCount")
    println(s"Customers        : $customerCount")
    println(s"Input partitions : $inputPartitions")
    println(s"Customer ID      : id % $customerCount")
    println("Aggregation       : groupBy(customerId).count()")

    println("-" * 90)
    println()
  }

  // ===========================================================================
  // SUMMARY
  // ===========================================================================

  /**
   * Prints the complete benchmark matrix.
   *
   * We deliberately avoid calling any configuration a "winner".
   *
   * The final analysis should combine these timings with Spark UI evidence.
   */
  private def printSummary(
                            results: Seq[ExperimentResult]
                          ): Unit = {

    println()
    println()
    println("=" * 90)
    println("MODULE 1.8.2 - BENCHMARK SUMMARY")
    println("=" * 90)

    println()

    println(
      f"${"Shuffle Partitions"}%-20s | " +
        f"${"Duration (ms)"}%-15s | " +
        f"${"Grouped Rows"}%-15s"
    )

    println("-" * 90)

    results.foreach { result =>

      println(
        f"${result.shufflePartitions}%-20d | " +
          f"${result.durationMs}%-15d | " +
          f"${result.resultRowCount}%-15d"
      )
    }

    println("-" * 90)

    println()
    println(
      "Interpret timing together with Spark UI partition sizes, task duration,"
    )
    println(
      "shuffle metrics, spill, memory, and AQE behavior."
    )

    println()
  }

  // ===========================================================================
  // PRODUCTION GUIDANCE
  // ===========================================================================

  private def printProductionGuidance(): Unit = {

    println()
    println("=" * 90)
    println("PRODUCTION ENGINEERING INTERPRETATION")
    println("=" * 90)

    println(
      """
        |
        |Too few shuffle partitions:
        |
        |  - Larger partitions
        |  - Larger tasks
        |  - Higher per-task memory pressure
        |  - Greater spill risk
        |  - Potential stragglers
        |
        |Too many shuffle partitions:
        |
        |  - Smaller partitions
        |  - More tasks
        |  - Greater scheduler overhead
        |  - More task-management overhead
        |
        |AQE:
        |
        |  - Inspects actual shuffle statistics
        |  - Can coalesce small shuffle partitions
        |  - Can make runtime partitioning different from the configured value
        |
        |Production investigation should inspect:
        |
        |  1. Configured shuffle partitions
        |  2. Initial physical plan
        |  3. Final adaptive physical plan
        |  4. Shuffle bytes
        |  5. Shuffle records
        |  6. Partition-size distribution
        |  7. Task duration distribution
        |  8. Spill
        |  9. GC
        | 10. AQE coalescing
        |
        |""".stripMargin
    )
  }

  // ===========================================================================
  // UI PAUSE
  // ===========================================================================

  /**
   * Keeps Spark alive while the user inspects the Spark UI.
   *
   * Two mechanisms are used:
   *
   *   1. Timeout thread
   *   2. ENTER key through stdin
   *
   * The Gradle JavaExec task must contain:
   *
   *   standardInput = System.`in`
   */
  private def pauseWithUi(
                           seconds: Int
                         ): Unit = {

    if (seconds <= 0) {
      return
    }

    val pauseCompleted =
      new java.util.concurrent.CountDownLatch(1)

    // -------------------------------------------------------------------------
    // Timeout thread.
    // -------------------------------------------------------------------------

    val timerThread =
      new Thread(
        new Runnable {
          override def run(): Unit = {

            try {
              Thread.sleep(seconds.toLong * 1000L)
            } catch {
              case _: InterruptedException =>
                // Expected when ENTER is pressed.
            } finally {
              pauseCompleted.countDown()
            }
          }
        },
        "shuffle-partition-ui-pause"
      )

    timerThread.setDaemon(true)
    timerThread.start()

    // -------------------------------------------------------------------------
    // Stdin thread.
    //
    // This allows:
    //
    //   ENTER
    //
    // to terminate the pause early.
    // -------------------------------------------------------------------------

    val inputThread =
      new Thread(
        new Runnable {
          override def run(): Unit = {

            try {

              val reader =
                new java.io.BufferedReader(
                  new java.io.InputStreamReader(System.in)
                )

              reader.readLine()

              timerThread.interrupt()
              pauseCompleted.countDown()

            } catch {
              case _: Exception =>
                // Ignore stdin termination.
            }
          }
        },
        "shuffle-partition-ui-stdin"
      )

    inputThread.setDaemon(true)
    inputThread.start()

    try {
      pauseCompleted.await()
    } catch {
      case _: InterruptedException =>
        Thread.currentThread().interrupt()
    }
  }

  // ===========================================================================
  // ARGUMENT PARSING
  // ===========================================================================

  /**
   * Converts:
   *
   *   64
   *
   * into:
   *
   *   Seq(64)
   *
   * Converts:
   *
   *   matrix
   *
   * into:
   *
   *   Seq(8, 16, 32, 64, 128, 256)
   *
   * Also supports:
   *
   *   matrix=8,16,32,64,128,256
   */
  private def parseExperimentSpec(
                                   value: String
                                 ): Seq[Int] = {

    val normalized =
      value.trim.toLowerCase

    if (normalized == "matrix") {

      Seq(8, 16, 32, 64, 128, 256)

    } else if (normalized.startsWith("matrix=")) {

      val matrixValues =
        normalized
          .stripPrefix("matrix=")
          .split(",")
          .map(_.trim)
          .filter(_.nonEmpty)
          .map { token =>
            parsePositiveInt(
              token,
              "shufflePartitions"
            )
          }

      if (matrixValues.isEmpty) {
        sys.error(
          "matrix must contain at least one partition count."
        )
      }

      matrixValues.distinct.toSeq

    } else {

      Seq(
        parsePositiveInt(
          normalized,
          "shufflePartitions"
        )
      )
    }
  }

  /**
   * Extract:
   *
   *   --ui-pause=120
   */
  private def extractUiPause(
                              optionalArgs: Array[String]
                            ): Int = {

    optionalArgs
      .find(_.startsWith("--ui-pause="))
      .map { value =>

        val rawValue =
          value
            .stripPrefix("--ui-pause=")
            .trim

        val seconds =
          try {
            rawValue.toInt
          } catch {
            case _: NumberFormatException =>
              sys.error(
                s"Invalid UI pause value: $value"
              )
          }

        if (seconds < 0 || seconds > 3600) {
          sys.error(
            "UI pause must be between 0 and 3600 seconds."
          )
        }

        seconds
      }
      .getOrElse(0)
  }

  // ===========================================================================
  // VALIDATION
  // ===========================================================================

  private def validateShufflePartitions(
                                         values: Seq[Int]
                                       ): Unit = {

    values.foreach { value =>

      if (value <= 0) {
        sys.error(
          s"Shuffle partition count must be greater than zero: $value"
        )
      }
    }
  }

  /**
   * Scala-version-compatible positive Long parser.
   *
   * We intentionally do not use String.toLongOption because the project's
   * Scala version does not expose that method.
   */
  private def parsePositiveLong(
                                 value: String,
                                 name: String
                               ): Long = {

    try {

      val parsed =
        value.toLong

      if (parsed <= 0) {
        sys.error(
          s"$name must be a positive number: $value"
        )
      }

      parsed

    } catch {

      case _: NumberFormatException =>
        sys.error(
          s"$name must be a positive number: $value"
        )
    }
  }

  /**
   * Scala-version-compatible positive Int parser.
   *
   * We intentionally do not use String.toIntOption because the project's
   * Scala version does not expose that method.
   */
  private def parsePositiveInt(
                                value: String,
                                name: String
                              ): Int = {

    try {

      val parsed =
        value.toInt

      if (parsed <= 0) {
        sys.error(
          s"$name must be a positive integer: $value"
        )
      }

      parsed

    } catch {

      case _: NumberFormatException =>
        sys.error(
          s"$name must be a positive integer: $value"
        )
    }
  }

  // ===========================================================================
  // USAGE
  // ===========================================================================

  private def printUsage(): Unit = {

    println(
      """
        |
        |Module 1.8.2 - Shuffle Partition Sizing
        |
        |Usage:
        |
        |Single experiment:
        |
        |  ./gradlew runShufflePartitionSizingExperiment \
        |    --args="10000000 1000000 6 64"
        |
        |Single experiment with UI pause:
        |
        |  ./gradlew runShufflePartitionSizingExperiment \
        |    --args="10000000 1000000 6 64 --ui-pause=120"
        |
        |Default matrix:
        |
        |  ./gradlew runShufflePartitionSizingExperiment \
        |    --args="10000000 1000000 6 matrix"
        |
        |Custom matrix:
        |
        |  ./gradlew runShufflePartitionSizingExperiment \
        |    --args="10000000 1000000 6 matrix=8,16,32,64,128,256"
        |
        |Arguments:
        |
        |  rowCount
        |  customerCount
        |  inputPartitions
        |  shufflePartitions | matrix | matrix=a,b,c,...
        |
        |Optional:
        |
        |  --ui-pause=<seconds>
        |
        |""".stripMargin
    )
  }
}
