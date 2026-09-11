package com.shrikant.spark.aggregations

import com.shrikant.spark.foundation.EnterpriseSparkSession
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions._

/**
 * Module 1.7.4 — Aggregation Cardinality
 *
 * Investigates how grouping-key cardinality affects Spark aggregation
 * resource consumption and execution characteristics.
 *
 * ---------------------------------------------------------------------------
 * Experiment objective
 * ---------------------------------------------------------------------------
 *
 * The previous experiment (Module 1.7.3) demonstrated that partial
 * aggregation becomes less effective as grouping-key cardinality increases.
 *
 * This experiment extends that investigation by running a controlled
 * cardinality matrix while keeping the input workload and shuffle
 * configuration constant.
 *
 * The experiment varies only the number of distinct grouping keys:
 *
 *   10M rows / 100K keys
 *   10M rows / 500K keys
 *   10M rows / 1M keys
 *   10M rows / 5M keys
 *   10M rows / 10M keys
 *
 * This allows us to observe how increasing aggregation cardinality affects:
 *
 *   - Partial aggregation output
 *   - Shuffle records
 *   - Shuffle bytes
 *   - Aggregation memory
 *   - Aggregation build time
 *   - Hash probing
 *   - Spill behavior
 *   - Sort fallback
 *   - AQE partition coalescing
 *
 * ---------------------------------------------------------------------------
 * Engineering questions
 * ---------------------------------------------------------------------------
 *
 *   1. How does aggregation state grow with grouping-key cardinality?
 *   2. How does cardinality affect partial aggregation effectiveness?
 *   3. How does cardinality affect shuffle volume?
 *   4. At what point does aggregation become significantly more expensive?
 *   5. When does aggregation memory become a production concern?
 *   6. Can the physical plan remain unchanged while runtime cost changes
 *      significantly?
 *
 * ---------------------------------------------------------------------------
 * Controlled variables
 * ---------------------------------------------------------------------------
 *
 * Input rows:
 *   10,000,000
 *
 * Shuffle partitions:
 *   20
 *
 * Input partitioning:
 *   Spark Range with its default partitioning for this local workload.
 *
 * Variable:
 *   Number of distinct grouping keys.
 *
 * ---------------------------------------------------------------------------
 * Important interpretation rule
 * ---------------------------------------------------------------------------
 *
 * Aggregation cardinality is not the same as input row count.
 *
 * A 10M-row dataset with 100K keys contains substantial key repetition.
 * A 10M-row dataset with 10M keys contains effectively one row per key.
 *
 * As cardinality approaches input-row count, partial aggregation has less
 * opportunity to collapse rows before the shuffle.
 *
 * The Spark UI remains the source of truth for actual:
 *
 *   - output rows
 *   - shuffle records
 *   - shuffle bytes
 *   - memory
 *   - execution time
 *   - spill
 *
 * ---------------------------------------------------------------------------
 * Execution
 * ---------------------------------------------------------------------------
 *
 * Run the complete matrix:
 *
 *   ./gradlew runAggregationCardinalityExperiment \
 *     --args="10000000 20 --ui-pause=300"
 *
 * Arguments:
 *
 *   arg 0 = transaction count
 *   arg 1 = shuffle partitions
 *   arg 2 = optional --ui-pause=<seconds>
 *
 * The experiment runs the following cardinalities:
 *
 *   100K
 *   500K
 *   1M
 *   5M
 *   10M
 *
 * ---------------------------------------------------------------------------
 * Production relevance
 * ---------------------------------------------------------------------------
 *
 * High-cardinality aggregations are common in workloads involving:
 *
 *   - transaction IDs
 *   - event IDs
 *   - account-level metrics
 *   - instrument-level metrics
 *   - customer/product combinations
 *   - fine-grained regulatory reporting
 *
 * These workloads can create large in-memory aggregation state and large
 * shuffle volumes even when the input dataset itself is not exceptionally
 * large.
 *
 * The engineering decision should therefore consider both:
 *
 *   input volume
 *
 * and:
 *
 *   aggregation cardinality.
 */
object AggregationCardinalityExperiment {

  /**
   * Entry point for the cardinality experiment matrix.
   */
  def main(args: Array[String]): Unit = {

    // -----------------------------------------------------------------------
    // 1. Parse experiment parameters
    // -----------------------------------------------------------------------

    val transactionCount: Long =
      if (args.nonEmpty) args(0).toLong
      else 10000000L

    val shufflePartitions: Int =
      if (args.length > 1) args(1).toInt
      else 20

    val uiPauseSeconds: Int =
      args
        .find(_.startsWith("--ui-pause="))
        .map(_.stripPrefix("--ui-pause=").toInt)
        .getOrElse(0)

    // -----------------------------------------------------------------------
    // 2. Validate parameters
    // -----------------------------------------------------------------------

    require(
      transactionCount > 0,
      "transactionCount must be greater than zero."
    )

    require(
      shufflePartitions > 0,
      "shufflePartitions must be greater than zero."
    )

    require(
      uiPauseSeconds >= 0,
      "uiPauseSeconds cannot be negative."
    )

    // -----------------------------------------------------------------------
    // 3. Define cardinality matrix
    // -----------------------------------------------------------------------
    //
    // Keep the input workload constant while increasing the number of
    // grouping keys.
    //
    // The final case intentionally uses one unique key per input row.
    // This represents the extreme case where partial aggregation has almost
    // no opportunity to reduce the input before the shuffle.
    //
    // -----------------------------------------------------------------------

    val cardinalities: Seq[Long] =
      Seq(
        100000L,
        500000L,
        1000000L,
        5000000L,
        10000000L
      )

    // -----------------------------------------------------------------------
    // 4. Create Spark session
    // -----------------------------------------------------------------------

    val spark: SparkSession =
      EnterpriseSparkSession.create(
        "Aggregation Cardinality Experiment",
        "dev"
      )

    // Keep shuffle parallelism constant across every experiment.
    //
    // This is important because changing shuffle partitions at the same time
    // as cardinality would introduce another experimental variable.
    spark.conf.set(
      "spark.sql.shuffle.partitions",
      shufflePartitions.toString
    )

    // -----------------------------------------------------------------------
    // 5. Print experiment configuration
    // -----------------------------------------------------------------------

    println()
    println("====================================================")
    println("MODULE 1.7.4 - AGGREGATION CARDINALITY")
    println("====================================================")

    println()
    println("EXPERIMENT CONFIGURATION")
    println("----------------------------------------------------")

    println(s"Transactions = $transactionCount")
    println(s"Shuffle partitions = $shufflePartitions")
    println(s"Cardinality levels = ${cardinalities.mkString(", ")}")
    println(s"UI pause = $uiPauseSeconds seconds")

    println()
    println("SPARK CONFIGURATION")
    println("----------------------------------------------------")

    println(
      s"spark.sql.adaptive.enabled = " +
        spark.conf.get("spark.sql.adaptive.enabled")
    )

    println(
      s"spark.sql.shuffle.partitions = " +
        spark.conf.get("spark.sql.shuffle.partitions")
    )

    // -----------------------------------------------------------------------
    // 6. Run the cardinality matrix
    // -----------------------------------------------------------------------
    //
    // Each cardinality is executed as a separate aggregation query.
    //
    // We deliberately keep the application alive after each query only when
    // --ui-pause is requested. For matrix execution, a long pause after every
    // experiment would make the complete run unnecessarily slow.
    //
    // Therefore the detailed Spark UI investigation should be performed on
    // the final query, while the console summary provides timing and plan
    // information for the complete matrix.
    //
    // -----------------------------------------------------------------------

    val results =
      cardinalities.map { customerCount =>

        println()
        println()
        println("====================================================")
        println(s"CARDINALITY = $customerCount")
        println("====================================================")

        val averageRowsPerKey =
          transactionCount.toDouble / customerCount.toDouble

        println(
          f"Average rows per grouping key = $averageRowsPerKey%.2f"
        )

        // ---------------------------------------------------------------
        // Create deterministic workload
        // ---------------------------------------------------------------
        //
        // Using modulo gives us a controlled number of grouping keys while
        // keeping the input row count unchanged.
        //
        // Example:
        //
        //   100K keys  → id % 100K
        //   1M keys    → id % 1M
        //   10M keys   → id % 10M
        //
        // This makes the cardinality effect reproducible.
        //
        // ---------------------------------------------------------------

        val transactionDf: DataFrame =
          spark.range(0, transactionCount)
            .select(
              col("id").alias("transaction_id"),
              (col("id") % customerCount).alias("customer_id"),
              (col("id") % 1000).alias("amount")
            )

        // ---------------------------------------------------------------
        // Define aggregation
        // ---------------------------------------------------------------

        val aggregatedDf: DataFrame =
          transactionDf
            .groupBy(col("customer_id"))
            .agg(
              sum(col("amount")).alias("total_amount")
            )

        // ---------------------------------------------------------------
        // Print initial physical plan
        // ---------------------------------------------------------------
        //
        // The first iteration establishes the expected physical strategy.
        // Subsequent iterations are expected to retain the same logical
        // aggregation structure while runtime characteristics change.
        //
        // ---------------------------------------------------------------

        if (customerCount == cardinalities.head) {

          println()
          println("INITIAL PHYSICAL PLAN")
          println("----------------------------------------------------")

          aggregatedDf.explain("formatted")
        }

        // ---------------------------------------------------------------
        // Execute the aggregation
        // ---------------------------------------------------------------
        //
        // foreachPartition is intentionally used instead of collect().
        //
        // collect() would transfer the complete aggregated result to the
        // driver and could distort the experiment, particularly for the
        // 10M-key case.
        //
        // foreachPartition forces execution while keeping result handling
        // distributed.
        //
        // ---------------------------------------------------------------

        val startTimeNanos =
          System.nanoTime()

        aggregatedDf.foreachPartition { rows: Iterator[Row] =>
          while (rows.hasNext) {
            rows.next()
          }
        }

        val executionSeconds =
          (System.nanoTime() - startTimeNanos) / 1e9

        // ---------------------------------------------------------------
        // Capture executed plan
        // ---------------------------------------------------------------

        val executedPlan =
          aggregatedDf.queryExecution.executedPlan.toString

        // ---------------------------------------------------------------
        // Print experiment result
        // ---------------------------------------------------------------

        println()
        println("RESULT")
        println("----------------------------------------------------")

        println(s"Grouping keys = $customerCount")

        println(
          f"Average rows per key = $averageRowsPerKey%.2f"
        )

        println(
          f"Application action time = $executionSeconds%.3f s"
        )

        println()
        println("EXECUTED PHYSICAL PLAN")
        println("----------------------------------------------------")

        println(executedPlan)

        // ---------------------------------------------------------------
        // Return lightweight result for matrix summary
        // ---------------------------------------------------------------

        (
          customerCount,
          averageRowsPerKey,
          executionSeconds
        )
      }

    // -----------------------------------------------------------------------
    // 7. Print matrix summary
    // -----------------------------------------------------------------------

    println()
    println()
    println("====================================================")
    println("MODULE 1.7.4 - CARDINALITY MATRIX SUMMARY")
    println("====================================================")

    println()
    println(
      f"${"Keys"}%-12s" +
        f"${"Rows/Key"}%-15s" +
        f"${"Action Time"}%-15s"
    )

    println("-" * 42)

    results.foreach {
      case (
        customerCount,
        averageRowsPerKey,
        executionSeconds
      ) =>

        println(
          f"$customerCount%-12d" +
            f"$averageRowsPerKey%-15.2f" +
            f"$executionSeconds%-15.3f"
        )
    }

    // -----------------------------------------------------------------------
    // 8. Print Spark UI instructions
    // -----------------------------------------------------------------------

    println()
    println("====================================================")
    println("SPARK UI INVESTIGATION")
    println("====================================================")

    println("http://localhost:4040")

    println()
    println("For each cardinality, capture from Query Details:")

    println("  - Partial HashAggregate output rows")
    println("  - Final HashAggregate output rows")
    println("  - Shuffle records written")
    println("  - Shuffle bytes written")
    println("  - Aggregation build time")
    println("  - Peak memory")
    println("  - Spill size")
    println("  - Sort fallback tasks")
    println("  - Average hash probes")
    println("  - AQE final partition count")

    println()
    println("Calculate partial aggregation reduction as:")

    println(
      "  1 - (shuffle records written / input records)"
    )

    // -----------------------------------------------------------------------
    // 9. Keep application alive for Spark UI investigation
    // -----------------------------------------------------------------------

    if (uiPauseSeconds > 0) {

      println()
      println("====================================================")
      println("SPARK UI PAUSE")
      println("====================================================")

      println(
        s"Spark UI will remain available for $uiPauseSeconds seconds."
      )

      println(
        "Open http://localhost:4040 and inspect the matrix queries."
      )

      Thread.sleep(uiPauseSeconds * 1000L)

      println("UI pause completed.")
    }

    // -----------------------------------------------------------------------
    // 10. Clean shutdown
    // -----------------------------------------------------------------------

    spark.stop()
  }
}
