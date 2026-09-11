package com.shrikant.spark.aggregations

import com.shrikant.spark.foundation.EnterpriseSparkSession
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

/**
 * Module 1.7.11
 *
 * Aggregation Strategy Comparison
 *
 * Objective:
 * Compare how Spark executes different aggregation patterns and how
 * aggregation cardinality / complexity affects:
 *
 *     - HashAggregate
 *     - ObjectHashAggregate
 *     - SortAggregate
 *     - Exchange / shuffle
 *     - AQE
 *     - AQEShuffleRead
 *     - execution time
 *     - number of groups
 *     - aggregation state size
 *     - memory pressure
 *     - scalability
 *
 * Experiments:
 *
 * A - COUNT(*)
 * B - SUM(amount)
 * C - AVG(amount)
 * D - MIN + MAX
 * E - Multiple aggregations
 * F - COUNT DISTINCT
 * G - Mixed aggregation + COUNT DISTINCT
 * H - Low cardinality aggregation: 10 groups
 * I - Medium cardinality aggregation: 10,000 groups
 * J - High cardinality aggregation: customer_id
 *
 * Important:
 *
 * The application intentionally pauses after all experiments.
 *
 * This keeps the Spark application alive so that Spark UI can be inspected
 * at:
 *
 * http://localhost:4040
 *
 * Press ENTER in the Gradle terminal only after screenshots / metrics have
 * been collected.
 */
object AggregationStrategyComparison {

  // ---------------------------------------------------------------------------
  // Configuration
  // ---------------------------------------------------------------------------

  case class Config(
                     numRows: Long,
                     numCustomers: Long,
                     shufflePartitions: Int
                   )

  // ---------------------------------------------------------------------------
  // Main
  // ---------------------------------------------------------------------------

  def main(args: Array[String]): Unit = {

    val config = parseAndValidateArgs(args)

    printExperimentHeader(config)

    val spark = createSparkSession(config)

    try {

      printSparkConfiguration(spark)

      // -----------------------------------------------------------------------
      // Generate deterministic transaction data.
      // -----------------------------------------------------------------------

      val transactions =
        generateTransactionData(
          spark,
          config.numRows,
          config.numCustomers
        )

      printDatasetStatistics(transactions)

      // -----------------------------------------------------------------------
      // Materialize/cache input data.
      //
      // This prevents the cost of repeatedly generating the synthetic input
      // dataset from dominating every aggregation experiment.
      // -----------------------------------------------------------------------

      materializeInput(transactions)

      // =======================================================================
      // A - COUNT
      // =======================================================================

      runExperiment(
        spark,
        "A - COUNT(*)",
        transactions
          .groupBy("customer_id")
          .agg(
            count("*").alias("transaction_count")
          )
      )

      // =======================================================================
      // B - SUM
      // =======================================================================

      runExperiment(
        spark,
        "B - SUM(amount)",
        transactions
          .groupBy("customer_id")
          .agg(
            sum("amount").alias("total_amount")
          )
      )

      // =======================================================================
      // C - AVG
      // =======================================================================

      runExperiment(
        spark,
        "C - AVG(amount)",
        transactions
          .groupBy("customer_id")
          .agg(
            avg("amount").alias("average_amount")
          )
      )

      // =======================================================================
      // D - MIN + MAX
      // =======================================================================

      runExperiment(
        spark,
        "D - MIN + MAX",
        transactions
          .groupBy("customer_id")
          .agg(
            min("amount").alias("minimum_amount"),
            max("amount").alias("maximum_amount")
          )
      )

      // =======================================================================
      // E - MULTIPLE AGGREGATIONS
      // =======================================================================
      //
      // This is closer to a realistic reporting workload where several
      // metrics are calculated for every business key.
      // =======================================================================

      runExperiment(
        spark,
        "E - MULTIPLE AGGREGATIONS",
        transactions
          .groupBy("customer_id")
          .agg(
            count("*").alias("transaction_count"),
            sum("amount").alias("total_amount"),
            avg("amount").alias("average_amount"),
            min("amount").alias("minimum_amount"),
            max("amount").alias("maximum_amount")
          )
      )

      // =======================================================================
      // F - COUNT DISTINCT PRODUCT
      // =======================================================================
      //
      // COUNT DISTINCT is intentionally included because it is generally more
      // expensive than a simple COUNT/SUM aggregation.
      //
      // Spark may require additional aggregation stages / state to implement
      // the distinct semantics.
      // =======================================================================

      runExperiment(
        spark,
        "F - COUNT DISTINCT product_id",
        transactions
          .groupBy("customer_id")
          .agg(
            countDistinct("product_id").alias("distinct_products")
          )
      )

      // =======================================================================
      // G - MIXED AGGREGATION + DISTINCT
      // =======================================================================

      runExperiment(
        spark,
        "G - COUNT + SUM + COUNT DISTINCT",
        transactions
          .groupBy("customer_id")
          .agg(
            count("*").alias("transaction_count"),
            sum("amount").alias("total_amount"),
            countDistinct("product_id").alias("distinct_products")
          )
      )

      // =======================================================================
      // H - LOW CARDINALITY
      // =======================================================================
      //
      // Only 10 groups are created.
      //
      // This allows us to observe how a very small aggregation state behaves.
      // =======================================================================

      val lowCardinalityData =
        transactions.withColumn(
          "low_cardinality_group",
          pmod(
            col("customer_id"),
            lit(10)
          )
        )

      runExperiment(
        spark,
        "H - LOW CARDINALITY (10 GROUPS)",
        lowCardinalityData
          .groupBy("low_cardinality_group")
          .agg(
            count("*").alias("transaction_count"),
            sum("amount").alias("total_amount")
          )
      )

      // =======================================================================
      // I - MEDIUM CARDINALITY
      // =======================================================================
      //
      // 10,000 aggregation groups.
      //
      // This represents a materially larger aggregation state than experiment
      // H while still remaining much smaller than customer-level aggregation.
      // =======================================================================

      val mediumCardinalityData =
        transactions.withColumn(
          "medium_cardinality_group",
          pmod(
            col("customer_id"),
            lit(10000)
          )
        )

      runExperiment(
        spark,
        "I - MEDIUM CARDINALITY (10K GROUPS)",
        mediumCardinalityData
          .groupBy("medium_cardinality_group")
          .agg(
            count("*").alias("transaction_count"),
            sum("amount").alias("total_amount")
          )
      )

      // =======================================================================
      // J - HIGH CARDINALITY
      // =======================================================================
      //
      // customer_id itself is used as the grouping key.
      //
      // With the default configuration:
      //
      //   1,000,000 customers
      //
      // this creates a very large number of aggregation groups.
      //
      // This experiment is especially useful for observing:
      //
      //   - aggregation state
      //   - memory usage
      //   - spill
      //   - task skew
      //   - shuffle behavior
      //   - AQE behavior
      // =======================================================================

      runExperiment(
        spark,
        "J - HIGH CARDINALITY (CUSTOMER_ID)",
        transactions
          .groupBy("customer_id")
          .agg(
            count("*").alias("transaction_count"),
            sum("amount").alias("total_amount")
          )
      )

      // -----------------------------------------------------------------------
      // Production guidance
      // -----------------------------------------------------------------------

      printProductionGuidance()

      // -----------------------------------------------------------------------
      // IMPORTANT:
      //
      // Keep the application alive so Spark UI remains accessible.
      //
      // Do NOT press ENTER until all screenshots / metrics have been captured.
      // -----------------------------------------------------------------------

      println()
      println("============================================================")
      println(" Module 1.7.11 experiments completed")
      println("============================================================")
      println()
      println("Spark UI is still available.")
      println()
      println("Open:")
      println("  http://localhost:4040")
      println()
      println("Capture the required Spark UI screenshots.")
      println()
      println("Press ENTER only after you finish inspecting the UI.")
      println("============================================================")
      println()

      scala.io.StdIn.readLine()

    } finally {

      // -----------------------------------------------------------------------
      // Clear cached data before Spark shuts down.
      // -----------------------------------------------------------------------

      try {
        transactionsUnpersistSafely(spark)
      } catch {
        case _: Throwable =>
          // Cleanup should never hide the actual experiment result.
      }

      // -----------------------------------------------------------------------
      // Spark UI disappears after SparkContext is stopped.
      //
      // Therefore spark.stop() intentionally occurs AFTER the ENTER pause.
      // -----------------------------------------------------------------------

      spark.stop()
    }
  }

  // ===========================================================================
  // Argument Parsing
  // ===========================================================================

  private def parseAndValidateArgs(args: Array[String]): Config = {

    if (args.length != 3) {

      println()
      println("Usage:")
      println(
        "./gradlew runAggregationStrategyComparison " +
          "--args=\"<numRows> <numCustomers> <shufflePartitions>\""
      )
      println()
      println("Example:")
      println(
        "./gradlew runAggregationStrategyComparison " +
          "--args=\"10000000 1000000 64\""
      )
      println()

      sys.exit(1)
    }

    try {

      val numRows =
        args(0).toLong

      val numCustomers =
        args(1).toLong

      val shufflePartitions =
        args(2).toInt

      require(
        numRows > 0,
        "numRows must be greater than zero."
      )

      require(
        numCustomers > 0,
        "numCustomers must be greater than zero."
      )

      require(
        shufflePartitions > 0,
        "shufflePartitions must be greater than zero."
      )

      require(
        numCustomers <= numRows,
        "numCustomers must be <= numRows for this experiment."
      )

      Config(
        numRows,
        numCustomers,
        shufflePartitions
      )

    } catch {

      case e: NumberFormatException =>

        println()
        println("Invalid numeric argument.")
        println(e.getMessage)
        println()

        sys.exit(1)

      case e: IllegalArgumentException =>

        println()
        println("Invalid configuration:")
        println(e.getMessage)
        println()

        sys.exit(1)
    }
  }

  // ===========================================================================
  // Spark Session
  // ===========================================================================

  private def createSparkSession(
                                  config: Config
                                ): SparkSession = {

    val spark =
      EnterpriseSparkSession.create(
        "AggregationStrategyComparison",
        "dev"
      )

    // -------------------------------------------------------------------------
    // Number of shuffle partitions.
    //
    // This is intentionally configurable because shuffle partition count has a
    // direct impact on aggregation performance and task parallelism.
    // -------------------------------------------------------------------------

    spark.conf.set(
      "spark.sql.shuffle.partitions",
      config.shufflePartitions
    )

    // -------------------------------------------------------------------------
    // AQE
    //
    // AQE allows Spark to modify parts of the physical execution plan at
    // runtime based on actual statistics.
    // -------------------------------------------------------------------------

    spark.conf.set(
      "spark.sql.adaptive.enabled",
      "true"
    )

    // -------------------------------------------------------------------------
    // AQE partition coalescing.
    //
    // Spark can combine small shuffle partitions after observing actual
    // runtime partition sizes.
    // -------------------------------------------------------------------------

    spark.conf.set(
      "spark.sql.adaptive.coalescePartitions.enabled",
      "true"
    )

    // -------------------------------------------------------------------------
    // Keep AQE skew handling enabled.
    //
    // Although this module is primarily about aggregation strategy, keeping
    // this enabled gives us a realistic production-like Spark configuration.
    // -------------------------------------------------------------------------

    spark.conf.set(
      "spark.sql.adaptive.skewJoin.enabled",
      "true"
    )

    spark
  }

  // ===========================================================================
  // Data Generation
  // ===========================================================================

  private def generateTransactionData(
                                       spark: SparkSession,
                                       numRows: Long,
                                       numCustomers: Long
                                     ): DataFrame = {

    // -------------------------------------------------------------------------
    // spark.range creates a distributed Long column named "id".
    //
    // The expressions below deliberately avoid random functions so that every
    // execution produces deterministic data.
    // -------------------------------------------------------------------------

    spark.range(numRows)
      .select(

        // Transaction identifier.
        col("id")
          .alias("transaction_id"),

        // Customer cardinality:
        //
        //   customer_id = id % numCustomers
        //
        // pmod is used instead of `%` so the expression is safe even if the
        // input could theoretically become negative.
        pmod(
          col("id"),
          lit(numCustomers)
        )
          .cast("long")
          .alias("customer_id"),

        // Product identifier.
        //
        // The multiplication creates a deterministic but different
        // distribution from customer_id.
        pmod(
          col("id") * lit(31L),
          lit(100000L)
        )
          .cast("long")
          .alias("product_id"),

        // Deterministic transaction amount.
        //
        // Range:
        //
        // approximately 0.00 -> 999.99
        //
        (pmod(
          col("id") * lit(17L),
          lit(100000L)
        )
          .cast("double") / lit(100.0))
          .alias("amount"),

        // ---------------------------------------------------------------------
        // IMPORTANT SPARK 3.5.1 FIX
        //
        // Do NOT do:
        //
        //   timestamp + bigint
        //
        // Spark 3.5.1 does not resolve TIMESTAMP + BIGINT as a valid binary
        // operation.
        //
        // timestampadd() explicitly adds seconds to the base timestamp.
        // ---------------------------------------------------------------------

        expr(
          "timestampadd(" +
            "SECOND, " +
            "CAST(pmod(id, 86400) AS INT), " +
            "TIMESTAMP '2026-01-01 00:00:00'" +
            ")"
        )
          .alias("transaction_ts")
      )
  }

  // ===========================================================================
  // Dataset Statistics
  // ===========================================================================

  private def printDatasetStatistics(
                                      transactions: DataFrame
                                    ): Unit = {

    println()
    println("============================================================")
    println(" Dataset Statistics")
    println("============================================================")

    println(
      s"Columns: ${transactions.columns.mkString(", ")}"
    )

    println()
    println("Schema:")

    transactions.printSchema()

    println(
      "Dataset generated successfully."
    )

    println()
  }

  // ===========================================================================
  // Materialize Input
  // ===========================================================================

  private def materializeInput(
                                transactions: DataFrame
                              ): Unit = {

    println()
    println("============================================================")
    println(" Materializing Input Dataset")
    println("============================================================")

    transactions.cache()

    val startTime =
      System.nanoTime()

    val rowCount =
      transactions.count()

    val durationSeconds =
      (System.nanoTime() - startTime) / 1e9

    println(
      f"Input rows: $rowCount%,d"
    )

    println(
      f"Materialization time: $durationSeconds%.2f seconds"
    )

    println()
  }

  // ===========================================================================
  // Experiment Runner
  // ===========================================================================

  private def runExperiment(
                             spark: SparkSession,
                             name: String,
                             aggregation: DataFrame
                           ): Unit = {

    println()
    println()
    println("============================================================")
    println(s" Experiment: $name")
    println("============================================================")

    // -------------------------------------------------------------------------
    // Display the optimized physical plan before execution.
    //
    // "formatted" is much easier to inspect than the raw explain output.
    // -------------------------------------------------------------------------

    println()
    println("Physical Plan:")
    println("------------------------------------------------------------")

    aggregation.explain("formatted")

    // -------------------------------------------------------------------------
    // Execute the aggregation.
    //
    // count() materializes the aggregation result and therefore forces Spark
    // to execute the complete aggregation query.
    // -------------------------------------------------------------------------

    println()
    println("Executing aggregation...")

    val startTime =
      System.nanoTime()

    val resultCount =
      aggregation.count()

    val durationSeconds =
      (System.nanoTime() - startTime) / 1e9

    println()
    println(
      f"Result groups: $resultCount%,d"
    )

    println(
      f"Execution time: $durationSeconds%.2f seconds"
    )

    // -------------------------------------------------------------------------
    // Print a small sample.
    //
    // IMPORTANT:
    //
    // We deliberately do NOT use orderBy() here.
    //
    // orderBy() would introduce an additional global sort and could therefore
    // distort our aggregation experiment.
    // -------------------------------------------------------------------------

    println()
    println("Sample result:")

    aggregation
      .limit(5)
      .show(
        truncate = false
      )

    // -------------------------------------------------------------------------
    // Inspect the executed physical plan.
    //
    // AQE can modify the physical plan after execution, so queryExecution's
    // executedPlan is especially useful for identifying adaptive operators.
    // -------------------------------------------------------------------------

    val executedPlan =
      aggregation.queryExecution.executedPlan.toString

    println()
    println("Operator Detection:")
    println("------------------------------------------------------------")

    printOperatorPresence(
      executedPlan,
      "HashAggregate",
      "HashAggregate"
    )

    printOperatorPresence(
      executedPlan,
      "ObjectHashAggregate",
      "ObjectHashAggregate"
    )

    printOperatorPresence(
      executedPlan,
      "SortAggregate",
      "SortAggregate"
    )

    printOperatorPresence(
      executedPlan,
      "Exchange",
      "Exchange"
    )

    printOperatorPresence(
      executedPlan,
      "AQEShuffleRead",
      "AQEShuffleRead"
    )

    println()
    println("Experiment completed.")
    println()
  }

  // ===========================================================================
  // Operator Detection
  // ===========================================================================

  private def printOperatorPresence(
                                     executedPlan: String,
                                     operator: String,
                                     displayName: String
                                   ): Unit = {

    val present =
      executedPlan.contains(operator)

    val status =
      if (present) "PRESENT"
      else "NOT PRESENT"

    println(
      f"$displayName%-22s : $status"
    )
  }

  // ===========================================================================
  // Spark Configuration
  // ===========================================================================

  private def printSparkConfiguration(
                                       spark: SparkSession
                                     ): Unit = {

    println()
    println("============================================================")
    println(" Spark Configuration")
    println("============================================================")

    println(
      s"Spark version: ${spark.version}"
    )

    println(
      s"Shuffle partitions: " +
        s"${spark.conf.get("spark.sql.shuffle.partitions")}"
    )

    println(
      s"AQE enabled: " +
        s"${spark.conf.get("spark.sql.adaptive.enabled")}"
    )

    println(
      s"AQE partition coalescing: " +
        s"${spark.conf.get("spark.sql.adaptive.coalescePartitions.enabled")}"
    )

    println(
      s"AQE skew join enabled: " +
        s"${spark.conf.get("spark.sql.adaptive.skewJoin.enabled")}"
    )

    println()
  }

  // ===========================================================================
  // Production Engineering Guidance
  // ===========================================================================

  private def printProductionGuidance(): Unit = {

    println()
    println("============================================================")
    println(" Production Engineering Takeaways")
    println("============================================================")

    println()
    println("1. Simple aggregations such as COUNT/SUM are generally cheaper")
    println("   than DISTINCT-based aggregations.")

    println()
    println("2. COUNT DISTINCT can require substantially more aggregation state")
    println("   and may create additional execution complexity.")

    println()
    println("3. Aggregation cardinality matters.")
    println("   Ten groups and one million groups have very different")
    println("   memory and scalability characteristics.")

    println()
    println("4. High-cardinality groupBy operations can increase")
    println("   aggregation state, memory pressure and spill risk.")

    println()
    println("5. AQE can change shuffle-read behavior at runtime based on")
    println("   the actual data distribution.")

    println()
    println("6. Physical-plan inspection should be combined with Spark UI")
    println("   metrics when diagnosing production aggregation performance.")

    println()
    println("7. Avoid adding unrelated ORDER BY / SORT operations when")
    println("   benchmarking aggregation itself.")

    println()
  }

  // ===========================================================================
  // Cleanup
  // ===========================================================================

  private def transactionsUnpersistSafely(
                                           spark: SparkSession
                                         ): Unit = {

    spark.catalog.clearCache()
  }

  // ===========================================================================
  // Header
  // ===========================================================================

  private def printExperimentHeader(
                                     config: Config
                                   ): Unit = {

    println()
    println("============================================================")
    println(" Module 1.7.11")
    println(" Aggregation Strategy Comparison")
    println("============================================================")
    println()

    println(
      f"Rows              : ${config.numRows}%,d"
    )

    println(
      f"Customers         : ${config.numCustomers}%,d"
    )

    println(
      f"Shuffle partitions: ${config.shufflePartitions}%,d"
    )

    println()
    println("Experiments:")
    println("  A - COUNT(*)")
    println("  B - SUM(amount)")
    println("  C - AVG(amount)")
    println("  D - MIN + MAX")
    println("  E - Multiple Aggregations")
    println("  F - COUNT DISTINCT")
    println("  G - Mixed Aggregation + COUNT DISTINCT")
    println("  H - Low Cardinality (10 groups)")
    println("  I - Medium Cardinality (10K groups)")
    println("  J - High Cardinality (customer_id)")
    println()
  }
}