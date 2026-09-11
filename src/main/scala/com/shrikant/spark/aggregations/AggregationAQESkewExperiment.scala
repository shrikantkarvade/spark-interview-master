package com.shrikant.spark.aggregations

import com.shrikant.spark.foundation.EnterpriseSparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * Module 1.7.10
 *
 * AQE + Aggregation Skew
 *
 * ============================================================================
 * PURPOSE
 * ============================================================================
 *
 * This experiment investigates how Adaptive Query Execution (AQE) behaves
 * when an aggregation workload contains a highly skewed key.
 *
 * The central production question is:
 *
 *   "If AQE is enabled, will Spark automatically solve aggregation skew?"
 *
 * The answer we want to establish experimentally is:
 *
 *   AQE can adapt shuffle partition behavior, but aggregation-key skew is
 *   fundamentally different from join skew and should not be assumed to be
 *   automatically split across multiple reducers.
 *
 * We will validate this using:
 *
 *   1. Console metrics
 *   2. Logical / physical plans
 *   3. Spark UI
 *   4. Stage/task distribution
 *   5. Runtime comparison
 *
 * ============================================================================
 * EXPERIMENT MATRIX
 * ============================================================================
 *
 * A. Uniform data + AQE OFF
 *    -> baseline
 *
 * B. Skewed data + AQE OFF
 *    -> establish the impact of skew
 *
 * C. Skewed data + AQE ON
 *    -> determine what AQE actually changes
 *
 * D. Skewed data + AQE ON + salted aggregation
 *    -> demonstrate a production mitigation technique
 *
 * ============================================================================
 * IMPORTANT CONCEPT
 * ============================================================================
 *
 * AQE has several independent capabilities:
 *
 *   - coalescing post-shuffle partitions
 *   - skew handling for joins
 *   - dynamic join strategy changes
 *   - local shuffle reader
 *
 * These capabilities should not be confused with automatically parallelizing
 * one logical aggregation key.
 *
 * Example:
 *
 *       groupBy("customer_id")
 *
 * If one customer represents a very large amount of data, all records for
 * that logical group ultimately need to be combined.
 *
 * Simply enabling AQE does not mean Spark will arbitrarily split:
 *
 *       HOT_CUSTOMER
 *
 * across multiple aggregation reducers while preserving the semantics of the
 * aggregation.
 *
 * ============================================================================
 */
object AggregationAQESkewExperiment {

  // --------------------------------------------------------------------------
  // Configuration
  // --------------------------------------------------------------------------

  private val DefaultRows = 5000000L
  private val DefaultCustomers = 1000000L
  private val DefaultHotKeyPercentage = 0.80
  private val DefaultShufflePartitions = 64

  /**
   * Controls whether the Spark UI should remain available after each
   * experiment so that the user can inspect Spark UI stages, tasks,
   * shuffle metrics, and execution details before continuing.
   *
   * CLI argument:
   *
   *   arg4 = UI pause
   *
   * Example:
   *
   *   true  -> pause after every experiment
   *   false -> execute all experiments continuously
   */
  private val DefaultUiPause = false

  /**
   * Entry point.
   *
   * CLI arguments:
   *
   *   arg0 = number of rows
   *   arg1 = number of customers
   *   arg2 = hot-key percentage
   *   arg3 = shuffle partitions
   *   arg4 = UI pause (true/false)
   *
   * Example:
   *
   *   ./gradlew runAggregationAQESkewExperiment \
   *     --args="5000000 1000000 0.80 64 true"
   *
   */
  def main(args: Array[String]): Unit = {

    val rows =
      if (args.length > 0) args(0).toLong
      else DefaultRows

    val customers =
      if (args.length > 1) args(1).toLong
      else DefaultCustomers

    val hotKeyPercentage =
      if (args.length > 2) args(2).toDouble
      else DefaultHotKeyPercentage

    val shufflePartitions =
      if (args.length > 3) args(3).toInt
      else DefaultShufflePartitions

    val uiPause =
      if (args.length > 4) args(4).toBoolean
      else DefaultUiPause

    validateArguments(
      rows,
      customers,
      hotKeyPercentage,
      shufflePartitions
    )

    // ------------------------------------------------------------------------
    // Create SparkSession using the project's standard EnterpriseSparkSession.
    // ------------------------------------------------------------------------

    val spark = EnterpriseSparkSession.create(
      "AggregationAQESkewExperiment",
      "dev"
    )

    try {

      configureSpark(
        spark,
        shufflePartitions
      )

      printConfiguration(spark)

      // ======================================================================
      // EXPERIMENT A
      // ======================================================================

      runExperiment(
        spark = spark,
        experimentName = "A - Uniform aggregation with AQE OFF",
        rows = rows,
        customers = customers,
        hotKeyPercentage = 0.0,
        shufflePartitions = shufflePartitions,
        aqeEnabled = false,
        salted = false,
        uiPause = uiPause
      )

      // ======================================================================
      // EXPERIMENT B
      // ======================================================================

      runExperiment(
        spark = spark,
        experimentName = "B - Skewed aggregation with AQE OFF",
        rows = rows,
        customers = customers,
        hotKeyPercentage = hotKeyPercentage,
        shufflePartitions = shufflePartitions,
        aqeEnabled = false,
        salted = false,
        uiPause = uiPause
      )

      // ======================================================================
      // EXPERIMENT C
      // ======================================================================

      runExperiment(
        spark = spark,
        experimentName = "C - Skewed aggregation with AQE ON",
        rows = rows,
        customers = customers,
        hotKeyPercentage = hotKeyPercentage,
        shufflePartitions = shufflePartitions,
        aqeEnabled = true,
        salted = false,
        uiPause = uiPause
      )

      // ======================================================================
      // EXPERIMENT D
      // ======================================================================

      runExperiment(
        spark = spark,
        experimentName = "D - Skewed aggregation with AQE ON + salting",
        rows = rows,
        customers = customers,
        hotKeyPercentage = hotKeyPercentage,
        shufflePartitions = shufflePartitions,
        aqeEnabled = true,
        salted = true,
        uiPause = uiPause
      )

    } finally {
      spark.stop()
    }
  }

  // ==========================================================================
  // ARGUMENT VALIDATION
  // ==========================================================================

  private def validateArguments(
                                 rows: Long,
                                 customers: Long,
                                 hotKeyPercentage: Double,
                                 shufflePartitions: Int
                               ): Unit = {

    require(
      rows > 0,
      "rows must be > 0"
    )

    require(
      customers > 0,
      "customers must be > 0"
    )

    require(
      hotKeyPercentage >= 0.0 &&
        hotKeyPercentage <= 1.0,
      "hotKeyPercentage must be between 0.0 and 1.0"
    )

    require(
      shufflePartitions > 0,
      "shufflePartitions must be > 0"
    )

    require(
      customers <= rows,
      "customers should not exceed rows for this experiment"
    )
  }

  // ==========================================================================
  // SPARK CONFIGURATION
  // ==========================================================================

  private def configureSpark(
                              spark: SparkSession,
                              shufflePartitions: Int
                            ): Unit = {

    spark.conf.set(
      "spark.sql.shuffle.partitions",
      shufflePartitions
    )

    /*
     * We deliberately leave AQE disabled initially.
     *
     * Each experiment explicitly changes:
     *
     *   spark.sql.adaptive.enabled
     *
     * before executing the query.
     */

    spark.conf.set(
      "spark.sql.adaptive.enabled",
      "false"
    )

    /*
     * These settings are useful later when examining AQE behavior.
     */

    spark.conf.set(
      "spark.sql.adaptive.coalescePartitions.enabled",
      "true"
    )

    /*
     * IMPORTANT:
     *
     * Spark's AQE skew-join feature is deliberately enabled so that we can
     * demonstrate an important distinction:
     *
     *     skew join optimization
     *
     * is not equivalent to:
     *
     *     skewed aggregation-key optimization.
     *
     * This experiment does NOT contain a join. Therefore, we should not expect
     * this setting to magically split a hot aggregation key.
     */

    spark.conf.set(
      "spark.sql.adaptive.skewJoin.enabled",
      "true"
    )
  }

  // ==========================================================================
  // EXPERIMENT DRIVER
  // ==========================================================================

  private def runExperiment(
                             spark: SparkSession,
                             experimentName: String,
                             rows: Long,
                             customers: Long,
                             hotKeyPercentage: Double,
                             shufflePartitions: Int,
                             aqeEnabled: Boolean,
                             salted: Boolean,
                             uiPause: Boolean
                           ): Unit = {

    println()
    println("=" * 100)
    println(experimentName)
    println("=" * 100)

    println(s"Rows                 : $rows")
    println(s"Customers            : $customers")
    println(s"Hot-key percentage   : $hotKeyPercentage")
    println(s"Shuffle partitions   : $shufflePartitions")
    println(s"AQE enabled          : $aqeEnabled")
    println(s"Salted aggregation   : $salted")
    println(s"UI pause             : $uiPause")
    println()

    // ------------------------------------------------------------------------
    // AQE is configured BEFORE query execution.
    //
    // AQE decisions are made during execution, so changing this configuration
    // after the query has already materialized would not give us the intended
    // experiment.
    // ------------------------------------------------------------------------

    spark.conf.set(
      "spark.sql.adaptive.enabled",
      aqeEnabled
    )

    // ------------------------------------------------------------------------
    // Generate the dataset.
    // ------------------------------------------------------------------------

    val input = generateDataset(
      spark = spark,
      rows = rows,
      customers = customers,
      hotKeyPercentage = hotKeyPercentage
    )

    println("Input partition count:")
    println(input.rdd.getNumPartitions)

    println()
    println("Input distribution:")
    showInputDistribution(input)

    // ------------------------------------------------------------------------
    // Materialize input.
    //
    // This separates dataset generation from the aggregation being measured.
    // ------------------------------------------------------------------------

    input.cache()

    val materializationStart = System.nanoTime()

    input.count()

    val materializationSeconds =
      (System.nanoTime() - materializationStart) / 1e9

    println(
      f"Input materialization time: $materializationSeconds%.2f seconds"
    )

    // ------------------------------------------------------------------------
    // Show the query plan BEFORE execution.
    //
    // This gives us the initial physical plan.
    // ------------------------------------------------------------------------

    println()
    println("Initial physical plan:")
    println("-" * 100)

    val aggregationPlan =
      if (salted) {
        buildSaltedAggregation(input)
      } else {
        buildNormalAggregation(input)
      }

    aggregationPlan.explain(true)

    println("-" * 100)

    // ------------------------------------------------------------------------
    // Execute the aggregation.
    //
    // count() forces the entire aggregation query to execute.
    // ------------------------------------------------------------------------

    val start = System.nanoTime()

    val resultCount =
      aggregationPlan.count()

    val elapsedSeconds =
      (System.nanoTime() - start) / 1e9

    println()
    println(s"Aggregation result groups: $resultCount")

    println(
      f"Aggregation execution time: $elapsedSeconds%.2f seconds"
    )

    // ------------------------------------------------------------------------
    // IMPORTANT:
    //
    // explain(true) AFTER execution allows us to inspect the final adaptive
    // physical plan.
    //
    // With AQE enabled, this may contain nodes such as:
    //
    //     AdaptiveSparkPlan
    //     AQEShuffleRead
    //
    // and may reveal partition coalescing decisions.
    // ------------------------------------------------------------------------

    println()
    println("Final physical plan after execution:")
    println("-" * 100)

    aggregationPlan.explain(true)

    println("-" * 100)

    // ------------------------------------------------------------------------
    // Show final aggregation output.
    //
    // We deliberately limit this output so the console remains readable.
    // ------------------------------------------------------------------------

    println()
    println("Sample aggregation result:")

    aggregationPlan
      .orderBy(desc("record_count"))
      .show(10, truncate = false)

    // ------------------------------------------------------------------------
    // Clean up cached input before the next experiment.
    // ------------------------------------------------------------------------

    input.unpersist()

    println()
    println(s"Completed: $experimentName")
    println("=" * 100)

    // ------------------------------------------------------------------------
    // OPTIONAL SPARK UI PAUSE
    //
    // Keep the Spark application alive so Spark UI can be inspected before
    // moving to the next experiment.
    //
    // IMPORTANT:
    //
    // This pause happens BEFORE the next experiment starts and BEFORE
    // spark.stop() is called.
    //
    // Therefore the Spark UI remains available during the pause.
    // ------------------------------------------------------------------------

    if (uiPause) {

      println()
      println("=" * 100)
      println(s"UI PAUSE: $experimentName")
      println("=" * 100)
      println()
      println("Spark UI is still running.")
      println("Inspect the Spark UI now.")
      println()
      println("Recommended evidence:")
      println("  - SQL / query execution")
      println("  - Stages")
      println("  - task distribution")
      println("  - shuffle read/write")
      println("  - partition count")
      println("  - task duration distribution")
      println()
      println("Press ENTER to continue to the next experiment...")
      println("=" * 100)

      scala.io.StdIn.readLine()
    }
  }

  // ==========================================================================
  // DATA GENERATION
  // ==========================================================================

  /**
   * Generates either uniform or highly skewed customer data.
   *
   * Uniform case:
   *
   *     rows are distributed across many customer IDs.
   *
   * Skewed case:
   *
   *     a configurable percentage of rows belongs to:
   *
   *         HOT_CUSTOMER
   *
   * Example:
   *
   *     5,000,000 rows
   *     80% hot-key percentage
   *
   * produces approximately:
   *
   *     4,000,000 rows -> HOT_CUSTOMER
   *     1,000,000 rows -> normal customers
   *
   * The purpose is not to model a specific business domain. The purpose is
   * to create a controlled skew distribution that we can observe in Spark.
   */
  private def generateDataset(
                               spark: SparkSession,
                               rows: Long,
                               customers: Long,
                               hotKeyPercentage: Double
                             ): DataFrame = {

    val hotRows =
      (rows.toDouble * hotKeyPercentage).toLong

    val normalRows =
      rows - hotRows

    // ------------------------------------------------------------------------
    // HOT KEY
    // ------------------------------------------------------------------------

    val hotData =
      spark.range(hotRows)
        .select(
          lit("HOT_CUSTOMER").as("customer_id"),
          (col("id") % 1000).cast("int").as("product_id"),
          (col("id") % 100).cast("int").as("region_id"),
          (col("id") + 1).cast("double").as("amount")
        )

    // ------------------------------------------------------------------------
    // NORMAL KEYS
    // ------------------------------------------------------------------------

    val normalData =
      spark.range(normalRows)
        .select(
          concat(
            lit("CUSTOMER_"),
            format_string(
              "%07d",
              (col("id") % customers).cast("long")
            )
          ).as("customer_id"),
          (col("id") % 1000).cast("int").as("product_id"),
          (col("id") % 100).cast("int").as("region_id"),
          (col("id") + 1).cast("double").as("amount")
        )

    hotData.unionByName(normalData)
  }

  // ==========================================================================
  // INPUT DISTRIBUTION
  // ==========================================================================

  private def showInputDistribution(
                                     input: DataFrame
                                   ): Unit = {

    println()

    input
      .groupBy("customer_id")
      .count()
      .orderBy(desc("count"))
      .show(10, truncate = false)
  }

  // ==========================================================================
  // NORMAL AGGREGATION
  // ==========================================================================

  /**
   * Standard aggregation:
   *
   *     GROUP BY customer_id
   *
   * Spark will normally construct an aggregation pipeline resembling:
   *
   *     HashAggregate
   *          |
   *     Exchange hashpartitioning(customer_id)
   *          |
   *     HashAggregate
   *
   * The Exchange is the important boundary.
   *
   * Rows with the same grouping key must ultimately reach the same
   * aggregation partition.
   */
  private def buildNormalAggregation(
                                      input: DataFrame
                                    ): DataFrame = {

    input
      .groupBy("customer_id")
      .agg(
        count(lit(1)).as("record_count"),
        sum("amount").as("total_amount")
      )
  }

  // ==========================================================================
  // SALTED AGGREGATION
  // ==========================================================================

  /**
   * Demonstrates the basic idea behind aggregation salting.
   *
   * Instead of immediately grouping everything by:
   *
   *     customer_id
   *
   * we introduce:
   *
   *     salt
   *
   * and initially aggregate by:
   *
   *     customer_id + salt
   *
   * This allows records belonging to one hot logical key to participate in
   * multiple physical aggregation groups.
   *
   * Conceptually:
   *
   * WITHOUT SALTING
   *
   *     HOT_CUSTOMER
   *          |
   *          +------------------------+
   *                                   |
   *                                   ▼
   *                              one reducer
   *
   *
   * WITH SALTING
   *
   *     HOT_CUSTOMER + 0 ───────► reducer
   *     HOT_CUSTOMER + 1 ───────► reducer
   *     HOT_CUSTOMER + 2 ───────► reducer
   *     HOT_CUSTOMER + 3 ───────► reducer
   *                    ...
   *
   * Then a second aggregation combines the partial results back to:
   *
   *     HOT_CUSTOMER
   *
   * This is a classic two-stage aggregation strategy.
   *
   * IMPORTANT:
   *
   * This is intentionally a teaching experiment, not yet a generic
   * production salting framework.
   */
  private def buildSaltedAggregation(
                                      input: DataFrame
                                    ): DataFrame = {

    val saltBuckets = 16

    val salted =
      input.withColumn(
        "salt",
        pmod(
          xxhash64(
            col("product_id"),
            col("region_id")
          ),
          lit(saltBuckets)
        )
      )

    val partial =
      salted
        .groupBy(
          col("customer_id"),
          col("salt")
        )
        .agg(
          count(lit(1)).as("partial_count"),
          sum("amount").as("partial_amount")
        )

    partial
      .groupBy("customer_id")
      .agg(
        sum("partial_count").as("record_count"),
        sum("partial_amount").as("total_amount")
      )
  }

  // ==========================================================================
  // CONFIGURATION OUTPUT
  // ==========================================================================

  private def printConfiguration(
                                  spark: SparkSession
                                ): Unit = {

    println()
    println("=" * 100)
    println("Spark Configuration")
    println("=" * 100)

    println(
      s"spark.sql.shuffle.partitions = " +
        spark.conf.get("spark.sql.shuffle.partitions")
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

    println(
      s"spark.sql.adaptive.skewJoin.enabled = " +
        spark.conf.get(
          "spark.sql.adaptive.skewJoin.enabled"
        )
    )

    println(
      s"UI pause = $DefaultUiPause"
    )

    println("=" * 100)
  }
}