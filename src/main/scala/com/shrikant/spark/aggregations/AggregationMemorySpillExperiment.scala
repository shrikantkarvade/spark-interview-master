package com.shrikant.spark.aggregations

import com.shrikant.spark.foundation.EnterpriseSparkSession
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

/**
 * Module 1.7.12
 *
 * Aggregation Memory, Spill & Hash Map Pressure
 *
 * ============================================================================
 * EXPERIMENT OBJECTIVE
 * ============================================================================
 *
 * This experiment investigates how Spark aggregation behaves when the number
 * of grouping keys increases and aggregation hash-map pressure grows.
 *
 * The experiment focuses on:
 *
 *   1. How HashAggregate stores grouping keys and aggregate state.
 *   2. Why high-cardinality aggregation consumes execution memory.
 *   3. How aggregation can trigger memory pressure and spilling.
 *   4. The relationship between partition size and task-level memory pressure.
 *   5. UnsafeFixedWidthAggregationMap-style aggregation behavior.
 *   6. The difference between memory pressure and data skew.
 *   7. How spill can increase CPU and disk I/O.
 *   8. Whether increasing shuffle partitions reduces per-task pressure.
 *   9. How AQE can change resulting partition sizes.
 *  10. How to distinguish:
 *        - OOM
 *        - spill
 *        - GC pressure
 *        - data skew
 *
 * IMPORTANT:
 *
 * A 10-million-row local experiment may NOT necessarily produce a spill.
 * Spill is an execution outcome, not something that should be artificially
 * forced. Spark UI metrics must be used to determine whether spilling occurred.
 *
 * ============================================================================
 * EXPERIMENT MATRIX
 * ============================================================================
 *
 * A - Low cardinality
 *     10M rows
 *     Low number of groups
 *     Base shuffle partitions
 *     AQE OFF
 *
 * B - Medium cardinality
 *     10M rows
 *     Medium number of groups
 *     Base shuffle partitions
 *     AQE OFF
 *
 * C - High cardinality
 *     10M rows
 *     High number of groups
 *     Base shuffle partitions
 *     AQE OFF
 *
 * D - Very high cardinality
 *     10M rows
 *     Very high number of groups
 *     Base shuffle partitions
 *     AQE OFF
 *
 * E - Very high cardinality + more partitions
 *     10M rows
 *     Very high number of groups
 *     2x shuffle partitions
 *     AQE OFF
 *
 * F - Very high cardinality + AQE
 *     10M rows
 *     Very high number of groups
 *     Base shuffle partitions
 *     AQE ON
 *
 * ============================================================================
 * SPARK UI VALIDATION
 * ============================================================================
 *
 * When --ui-pause=N is supplied, execution pauses after every experiment.
 *
 * During the pause inspect:
 *
 *   Spark UI:
 *     http://localhost:4040
 *
 *   SQL tab
 *     -> Current query
 *     -> Query details
 *     -> Physical plan
 *
 *   Stages tab
 *     -> Task duration
 *     -> Shuffle Read
 *     -> Shuffle Write
 *     -> Peak Execution Memory
 *     -> Spill (Memory)
 *     -> Spill (Disk)
 *     -> Input / Output
 *
 * Also inspect:
 *
 *   - Number of tasks
 *   - Task duration distribution
 *   - Whether a few tasks are significantly slower
 *   - Whether AQE changes partition count
 *   - Whether increasing shuffle partitions reduces task pressure
 *
 * ============================================================================
 * PRODUCTION ENGINEERING CONTEXT
 * ============================================================================
 *
 * Aggregation memory pressure is common in:
 *
 *   - customer-level aggregations
 *   - transaction processing
 *   - risk calculations
 *   - regulatory reporting
 *   - event processing
 *   - telemetry
 *   - fraud detection
 *   - financial analytics
 *
 * The production goal is NOT simply:
 *
 *     "Increase memory until the job works."
 *
 * Instead, engineers should understand:
 *
 *     data volume
 *          +
 *     cardinality
 *          +
 *     partition sizing
 *          +
 *     aggregation strategy
 *          +
 *     memory pressure
 *          +
 *     spill behavior
 *          +
 *     AQE
 *
 * ============================================================================
 */
object AggregationMemorySpillExperiment {

  /**
   * Configuration for one experiment.
   *
   * @param name               Human-readable experiment name.
   * @param rows               Number of input rows.
   * @param groups             Number of distinct grouping keys.
   * @param shufflePartitions  Spark SQL shuffle partition count.
   * @param aqeEnabled         Whether Adaptive Query Execution is enabled.
   */
  private case class ExperimentConfig(
                                       name: String,
                                       rows: Long,
                                       groups: Long,
                                       shufflePartitions: Int,
                                       aqeEnabled: Boolean
                                     )

  def main(args: Array[String]): Unit = {

    // ------------------------------------------------------------------------
    // Parse command-line arguments.
    //
    // Positional arguments:
    //
    //   0 -> rows
    //   1 -> groups
    //   2 -> shuffle partitions
    //
    // Optional argument:
    //
    //   --ui-pause=<seconds>
    //
    // Example:
    //
    //   --args="10000000 1000000 32 --ui-pause=60"
    // ------------------------------------------------------------------------

    val rows =
      parseLongArgument(
        args,
        index = 0,
        defaultValue = 10000000L,
        name = "rows"
      )

    val groups =
      parseLongArgument(
        args,
        index = 1,
        defaultValue = 1000000L,
        name = "groups"
      )

    val shufflePartitions =
      parseIntArgument(
        args,
        index = 2,
        defaultValue = 32,
        name = "shufflePartitions"
      )

    // ------------------------------------------------------------------------
    // Optional Spark UI inspection pause.
    //
    // This is intentionally implemented using a named option rather than
    // another positional argument so existing experiment commands remain
    // compatible.
    //
    // Example:
    //
    //   --ui-pause=60
    //
    // means:
    //
    //   Pause for 60 seconds after every experiment.
    // ------------------------------------------------------------------------

    val uiPauseSeconds =
      args
        .find(_.startsWith("--ui-pause="))
        .map(_.stripPrefix("--ui-pause=").toInt)
        .getOrElse(0)

    // ------------------------------------------------------------------------
    // Validate all input arguments before starting Spark.
    // ------------------------------------------------------------------------

    validateArguments(
      rows = rows,
      groups = groups,
      shufflePartitions = shufflePartitions,
      uiPauseSeconds = uiPauseSeconds
    )

    // ------------------------------------------------------------------------
    // Create the enterprise-standard SparkSession.
    //
    // This follows the same project convention used by the other
    // aggregation experiments.
    // ------------------------------------------------------------------------

    val spark =
      EnterpriseSparkSession.create(
        appName = "Module-1.7.12-Aggregation-Memory-Spill",
        environment = "dev"
      )

    try {

      // ----------------------------------------------------------------------
      // Print experiment configuration.
      // ----------------------------------------------------------------------

      printExperimentHeader(
        rows = rows,
        groups = groups,
        shufflePartitions = shufflePartitions,
        uiPauseSeconds = uiPauseSeconds
      )

      // ----------------------------------------------------------------------
      // Create the base dataset.
      //
      // The base DataFrame contains:
      //
      //   id
      //   customer_id
      //   amount
      //
      // The customer_id distribution is controlled later through modulo
      // arithmetic so that we can vary aggregation cardinality.
      // ----------------------------------------------------------------------

      val baseDf =
        createBaseData(
          spark = spark,
          rows = rows
        )

      // ----------------------------------------------------------------------
      // Materialize the generated input dataset.
      //
      // This confirms that the synthetic input can be generated successfully
      // before running the actual aggregation experiments.
      // ----------------------------------------------------------------------

      val materializationStart = System.nanoTime()

      baseDf.count()

      val materializationSeconds =
        elapsedSeconds(materializationStart)

      println()
      println(
        f"Base dataset materialization time: " +
          f"$materializationSeconds%.3f seconds"
      )

      // ----------------------------------------------------------------------
      // Print schema so that the input structure is visible.
      // ----------------------------------------------------------------------

      println()
      println("Base DataFrame schema:")
      baseDf.printSchema()

      // ----------------------------------------------------------------------
      // Print a small sample.
      //
      // This is useful for confirming that the generated data looks correct.
      // ----------------------------------------------------------------------

      println()
      println("Base DataFrame sample:")
      baseDf.show(
        numRows = 5,
        truncate = false
      )

      // ----------------------------------------------------------------------
      // Build the complete experiment matrix.
      // ----------------------------------------------------------------------

      val experiments =
        buildExperimentMatrix(
          rows = rows,
          groups = groups,
          shufflePartitions = shufflePartitions
        )

      // ----------------------------------------------------------------------
      // Execute each experiment.
      //
      // IMPORTANT:
      //
      // The UI pause occurs AFTER each experiment.
      //
      // Therefore:
      //
      //   Experiment
      //       ↓
      //   Spark action completes
      //       ↓
      //   Spark UI inspection pause
      //       ↓
      //   Next experiment
      //
      // This allows the user to inspect the completed query/stage metrics
      // before the next experiment changes the Spark UI state.
      // ----------------------------------------------------------------------

      experiments.foreach { config =>

        runExperiment(
          spark = spark,
          baseDf = baseDf,
          config = config
        )

        // --------------------------------------------------------------
        // Pause after every experiment when requested.
        // --------------------------------------------------------------

        pauseIfRequested(
          seconds = uiPauseSeconds
        )
      }

      // ----------------------------------------------------------------------
      // Final completion message.
      // ----------------------------------------------------------------------

      println()
      println("=" * 100)
      println("MODULE 1.7.12 EXPERIMENT COMPLETE")
      println("=" * 100)

      println()
      println("Key evidence to record from Spark UI:")
      println("  - Physical aggregation operator")
      println("  - Number of shuffle partitions")
      println("  - Number of output partitions")
      println("  - Peak Execution Memory")
      println("  - Spill (Memory)")
      println("  - Spill (Disk)")
      println("  - Shuffle Read")
      println("  - Shuffle Write")
      println("  - Task duration distribution")
      println("  - AQE partition coalescing")
      println("  - Any signs of GC or task imbalance")

      println()
      println("Recommended next step:")
      println(
        "Compare memory pressure as grouping-key cardinality increases " +
          "and determine whether additional shuffle partitions reduce " +
          "per-task aggregation pressure."
      )

    } finally {

      // ----------------------------------------------------------------------
      // Spark is stopped ONLY after all experiments and UI pauses finish.
      //
      // This is important because stopping Spark would immediately terminate
      // the Spark application and make the Spark UI unavailable.
      // ----------------------------------------------------------------------

      spark.stop()
    }
  }

  // ==========================================================================
  // BASE DATA GENERATION
  // ==========================================================================

  /**
   * Creates the synthetic base dataset.
   *
   * The dataset contains:
   *
   *   id
   *   customer_id
   *   amount
   *
   * The customer_id itself is generated deterministically.
   *
   * We deliberately keep the input generation independent of the aggregation
   * cardinality so that the same input can be reused across experiments.
   */
  private def createBaseData(
                              spark: SparkSession,
                              rows: Long
                            ): DataFrame = {

    import spark.implicits._

    spark
      .range(rows)
      .select(
        col("id"),
        pmod(
          col("id"),
          lit(1000000L)
        ).alias("customer_id"),
        (
          (col("id") % lit(1000L)).cast("double") + lit(1.0)
          ).alias("amount")
      )
  }

  // ==========================================================================
  // EXPERIMENT MATRIX
  // ==========================================================================

  /**
   * Builds the Module 1.7.12 experiment matrix.
   *
   * The requested number of groups is capped at the number of rows because
   * having more distinct groups than input rows is not meaningful for this
   * experiment.
   *
   * Cardinality levels are derived from the user-provided groups value.
   */
  private def buildExperimentMatrix(
                                     rows: Long,
                                     groups: Long,
                                     shufflePartitions: Int
                                   ): Seq[ExperimentConfig] = {

    val lowGroups =
      math.max(
        1L,
        math.min(groups, math.max(10L, groups / 100L))
      )

    val mediumGroups =
      math.max(
        lowGroups,
        math.min(groups, math.max(1000L, groups / 10L))
      )

    val highGroups =
      math.max(
        mediumGroups,
        math.min(groups, math.max(10000L, groups / 2L))
      )

    val veryHighGroups =
      math.min(
        rows,
        groups
      )

    Seq(
      // ----------------------------------------------------------------------
      // A — Low cardinality
      // ----------------------------------------------------------------------
      ExperimentConfig(
        name = "A - Low Cardinality",
        rows = rows,
        groups = lowGroups,
        shufflePartitions = shufflePartitions,
        aqeEnabled = false
      ),

      // ----------------------------------------------------------------------
      // B — Medium cardinality
      // ----------------------------------------------------------------------
      ExperimentConfig(
        name = "B - Medium Cardinality",
        rows = rows,
        groups = mediumGroups,
        shufflePartitions = shufflePartitions,
        aqeEnabled = false
      ),

      // ----------------------------------------------------------------------
      // C — High cardinality
      // ----------------------------------------------------------------------
      ExperimentConfig(
        name = "C - High Cardinality",
        rows = rows,
        groups = highGroups,
        shufflePartitions = shufflePartitions,
        aqeEnabled = false
      ),

      // ----------------------------------------------------------------------
      // D — Very high cardinality
      // ----------------------------------------------------------------------
      ExperimentConfig(
        name = "D - Very High Cardinality",
        rows = rows,
        groups = veryHighGroups,
        shufflePartitions = shufflePartitions,
        aqeEnabled = false
      ),

      // ----------------------------------------------------------------------
      // E — Very high cardinality with twice the shuffle partitions.
      //
      // Hypothesis:
      //
      // More shuffle partitions should generally reduce the amount of data
      // processed by each individual reduce-side task.
      //
      // This can reduce task-level memory pressure, although it can also
      // increase scheduling and shuffle overhead.
      // ----------------------------------------------------------------------
      ExperimentConfig(
        name = "E - Very High Cardinality + 2x Partitions",
        rows = rows,
        groups = veryHighGroups,
        shufflePartitions = shufflePartitions * 2,
        aqeEnabled = false
      ),

      // ----------------------------------------------------------------------
      // F — Very high cardinality with AQE enabled.
      //
      // Hypothesis:
      //
      // AQE may change the effective number/size of post-shuffle partitions.
      //
      // The actual result must be validated using the Spark UI and physical
      // plan rather than assumed.
      // ----------------------------------------------------------------------
      ExperimentConfig(
        name = "F - Very High Cardinality + AQE",
        rows = rows,
        groups = veryHighGroups,
        shufflePartitions = shufflePartitions,
        aqeEnabled = true
      )
    )
  }

  // ==========================================================================
  // EXPERIMENT EXECUTION
  // ==========================================================================

  /**
   * Executes one aggregation experiment.
   *
   * The experiment:
   *
   *   1. Configures Spark SQL settings.
   *   2. Creates the requested grouping key.
   *   3. Performs a SUM aggregation.
   *   4. Prints the physical plan.
   *   5. Executes the aggregation.
   *   6. Reports execution time.
   *   7. Displays a small result sample.
   *
   * The physical plan is printed before execution so that we can inspect the
   * planned aggregation strategy independently of the runtime metrics.
   */
  private def runExperiment(
                             spark: SparkSession,
                             baseDf: DataFrame,
                             config: ExperimentConfig
                           ): Unit = {

    println()
    println("=" * 100)
    println(s"EXPERIMENT: ${config.name}")
    println("=" * 100)

    println()
    println("Configuration:")
    println(s"  Rows               : ${config.rows}")
    println(s"  Groups             : ${config.groups}")
    println(s"  Shuffle partitions : ${config.shufflePartitions}")
    println(s"  AQE enabled        : ${config.aqeEnabled}")

    // ------------------------------------------------------------------------
    // Configure shuffle partitions for this experiment.
    // ------------------------------------------------------------------------

    spark.conf.set(
      "spark.sql.shuffle.partitions",
      config.shufflePartitions
    )

    // ------------------------------------------------------------------------
    // Configure AQE.
    //
    // We explicitly set the value for every experiment so that one experiment
    // cannot accidentally inherit the setting of a previous experiment.
    // ------------------------------------------------------------------------

    spark.conf.set(
      "spark.sql.adaptive.enabled",
      config.aqeEnabled
    )

    // ------------------------------------------------------------------------
    // Create the grouping key.
    //
    // Using modulo allows the same 10M-row input to produce different
    // aggregation cardinalities.
    //
    // Example:
    //
    //   groups = 100
    //
    // produces approximately 100 possible grouping keys.
    //
    // Whereas:
    //
    //   groups = 10,000,000
    //
    // approaches one grouping key per row.
    // ------------------------------------------------------------------------

    val aggregationInput =
      baseDf.select(
        pmod(
          col("id"),
          lit(config.groups)
        ).alias("group_id"),
        col("amount")
      )

    // ------------------------------------------------------------------------
    // Perform the aggregation.
    //
    // SUM(amount) GROUP BY group_id
    //
    // This is intentionally simple so that memory behavior of the aggregation
    // itself remains the main subject of investigation.
    // ------------------------------------------------------------------------

    val result =
      aggregationInput
        .groupBy("group_id")
        .agg(
          sum("amount").alias("total_amount")
        )

    // ------------------------------------------------------------------------
    // Print the physical execution plan.
    //
    // Look specifically for:
    //
    //   HashAggregate
    //   Exchange
    //   Partial aggregation
    //   Final aggregation
    //
    // The plan may also contain adaptive-query-execution nodes when AQE is
    // enabled.
    // ------------------------------------------------------------------------

    println()
    println("Physical plan:")
    result.explain(mode = "formatted")

    // ------------------------------------------------------------------------
    // Execute the aggregation and measure elapsed wall-clock time.
    //
    // count() forces Spark to execute the aggregation.
    // ------------------------------------------------------------------------

    println()
    println("Executing aggregation...")

    val start = System.nanoTime()

    val resultCount =
      result.count()

    val elapsed =
      elapsedSeconds(start)

    println()
    println(
      f"Aggregation execution time: $elapsed%.3f seconds"
    )

    println(
      s"Aggregation result row count: $resultCount"
    )

    // ------------------------------------------------------------------------
    // Display a small sample.
    //
    // This is intentionally limited so that the console is not flooded with
    // potentially millions of grouping keys.
    // ------------------------------------------------------------------------

    println()
    println("Aggregation result sample:")

    result
      .orderBy(col("group_id"))
      .show(
        numRows = 5,
        truncate = false
      )

    // ------------------------------------------------------------------------
    // Runtime reminder for Spark UI inspection.
    // ------------------------------------------------------------------------

    println()
    println("Spark UI validation checklist:")
    println("  - SQL tab")
    println("  - Query details")
    println("  - Physical plan")
    println("  - HashAggregate operators")
    println("  - Exchange operators")
    println("  - Shuffle Read")
    println("  - Shuffle Write")
    println("  - Peak Execution Memory")
    println("  - Spill (Memory)")
    println("  - Spill (Disk)")
    println("  - Task duration")
    println("  - Number of tasks")
    println("  - AQE partition changes")

    println()
    println("=" * 100)
    println(s"EXPERIMENT FINISHED: ${config.name}")
    println("=" * 100)
  }

  // ==========================================================================
  // SPARK UI PAUSE
  // ==========================================================================

  /**
   * Pauses execution so that the Spark UI can be inspected.
   *
   * This is an established convention across the Module 1.7 experiments.
   *
   * Example:
   *
   *   --ui-pause=60
   *
   * causes a 60-second inspection window after every experiment.
   *
   * IMPORTANT:
   *
   * spark.stop() is intentionally NOT called here.
   *
   * The Spark application remains alive during this pause, so:
   *
   *   http://localhost:4040
   *
   * remains available.
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
      println(
        s"Execution will resume automatically after $seconds seconds..."
      )

      // ----------------------------------------------------------------------
      // Keep the Spark application alive during the inspection window.
      // ----------------------------------------------------------------------

      Thread.sleep(
        seconds * 1000L
      )

      println()
      println("UI pause completed. Continuing experiment...")
      println("=" * 100)
    }
  }

  // ==========================================================================
  // ARGUMENT VALIDATION
  // ==========================================================================

  /**
   * Validates all command-line arguments.
   *
   * Validation happens before Spark starts so that invalid experiment
   * configurations fail fast.
   */
  private def validateArguments(
                                 rows: Long,
                                 groups: Long,
                                 shufflePartitions: Int,
                                 uiPauseSeconds: Int
                               ): Unit = {

    require(
      rows > 0,
      "rows must be greater than 0"
    )

    require(
      groups > 0,
      "groups must be greater than 0"
    )

    require(
      groups <= rows,
      "groups must be less than or equal to rows"
    )

    require(
      shufflePartitions > 0,
      "shufflePartitions must be greater than 0"
    )

    require(
      uiPauseSeconds >= 0,
      "uiPauseSeconds must be greater than or equal to 0"
    )
  }

  // ==========================================================================
  // ARGUMENT PARSING
  // ==========================================================================

  /**
   * Parses a Long positional argument.
   *
   * If the argument is not provided, the supplied default value is used.
   */
  private def parseLongArgument(
                                 args: Array[String],
                                 index: Int,
                                 defaultValue: Long,
                                 name: String
                               ): Long = {

    args
      .lift(index)
      .filterNot(_.startsWith("--"))
      .map { value =>
        try {
          value.toLong
        } catch {
          case _: NumberFormatException =>
            throw new IllegalArgumentException(
              s"Invalid $name value: '$value'. Expected a Long."
            )
        }
      }
      .getOrElse(defaultValue)
  }

  /**
   * Parses an Int positional argument.
   *
   * If the argument is not provided, the supplied default value is used.
   */
  private def parseIntArgument(
                                args: Array[String],
                                index: Int,
                                defaultValue: Int,
                                name: String
                              ): Int = {

    args
      .lift(index)
      .filterNot(_.startsWith("--"))
      .map { value =>
        try {
          value.toInt
        } catch {
          case _: NumberFormatException =>
            throw new IllegalArgumentException(
              s"Invalid $name value: '$value'. Expected an Int."
            )
        }
      }
      .getOrElse(defaultValue)
  }

  // ==========================================================================
  // TIMING
  // ==========================================================================

  /**
   * Converts elapsed nanoseconds to seconds.
   */
  private def elapsedSeconds(
                              startNanos: Long
                            ): Double = {

    (System.nanoTime() - startNanos) / 1e9
  }

  // ==========================================================================
  // EXPERIMENT HEADER
  // ==========================================================================

  /**
   * Prints the overall experiment configuration.
   */
  private def printExperimentHeader(
                                     rows: Long,
                                     groups: Long,
                                     shufflePartitions: Int,
                                     uiPauseSeconds: Int
                                   ): Unit = {

    println()
    println("=" * 100)
    println("MODULE 1.7.12 — AGGREGATION MEMORY, SPILL & HASH MAP PRESSURE")
    println("=" * 100)

    println()
    println("Base configuration:")
    println(s"  Rows               : $rows")
    println(s"  Groups             : $groups")
    println(s"  Shuffle partitions : $shufflePartitions")
    println(s"  UI pause seconds   : $uiPauseSeconds")

    println()
    println("Experiment purpose:")
    println("  - Investigate aggregation hash-map memory pressure")
    println("  - Compare low vs high aggregation cardinality")
    println("  - Observe execution memory")
    println("  - Observe memory/disk spill when it occurs")
    println("  - Compare different shuffle partition counts")
    println("  - Compare AQE OFF vs AQE ON")
    println("  - Distinguish memory pressure from data skew")

    println()
    println("Spark UI:")
    println("  http://localhost:4040")

    println()
    println("=" * 100)
  }
}
