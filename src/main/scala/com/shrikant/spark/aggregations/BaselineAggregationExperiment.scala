package com.shrikant.spark.aggregations

import com.shrikant.spark.foundation.EnterpriseSparkSession
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions._

/**
 * Module 1.7.1 — Baseline Aggregation
 *
 * Demonstrates how Spark executes a large groupBy + SUM aggregation.
 *
 * Workload:
 *
 * Transactions:
 *     - 10,000,000 rows
 *     - 100,000 customers
 *
 * Aggregation:
 *
 * SUM(amount) GROUP BY customer_id
 *
 * Expected physical execution:
 *
 * HashAggregate
 * ↓
 * Exchange
 * ↓
 * HashAggregate
 *
 * The first HashAggregate performs partial aggregation before the
 * shuffle. The Exchange redistributes the partial results by customer_id.
 * The second HashAggregate combines those partial results into the final
 * customer-level totals.
 *
 * Engineering lesson:
 *
 * Spark does not necessarily shuffle every input row independently.
 * Partial aggregation can significantly reduce shuffle volume when
 * multiple input rows share the same grouping key.
 *
 * The Spark UI should be used to validate this behavior through:
 *
 *   - shuffle read/write
 *   - number of input/output records
 *   - aggregation time
 *   - task duration
 *   - spill metrics
 *
 * This experiment establishes the baseline before investigating:
 *
 *   - aggregation cardinality
 *   - shuffle partition sizing
 *   - AQE coalescing
 *   - ordering after aggregation
 *   - Top-N aggregation
 *   - aggregation skew
 *   - production optimization
 *
 * Optional argument:
 *
 * --ui-pause=<seconds>
 *
 * Example:
 *
 * --ui-pause=300
 *
 * Keeps the Spark application alive after execution so that the Spark UI
 * remains available for runtime investigation.
 */
object BaselineAggregationExperiment {

  def main(args: Array[String]): Unit = {

    // -----------------------------------------------------------------------
    // 1. Parse experiment parameters
    // -----------------------------------------------------------------------

    val transactionCount: Long =
      if (args.nonEmpty) args(0).toLong
      else 10000000L

    val customerCount: Long =
      if (args.length > 1) args(1).toLong
      else 100000L

    val shufflePartitions: Int =
      if (args.length > 2) args(2).toInt
      else 20

    // Optional UI pause.
    //
    // This is intentionally separate from the Spark configuration because
    // it controls the lifetime of the local application after the workload
    // has completed. It allows the Spark UI to remain available for manual
    // inspection of stages, tasks and shuffle metrics.
    val uiPauseSeconds: Int =
      args
        .find(_.startsWith("--ui-pause="))
        .map(_.stripPrefix("--ui-pause=").toInt)
        .getOrElse(0)

    // -----------------------------------------------------------------------
    // 2. Create Spark session
    // -----------------------------------------------------------------------

    val spark: SparkSession =
      EnterpriseSparkSession.create(
        "Baseline Aggregation Experiment",
        "dev"
      )

    // Keep shuffle parallelism explicit so that subsequent experiments
    // can compare aggregation behavior under controlled conditions.
    spark.conf.set(
      "spark.sql.shuffle.partitions",
      shufflePartitions.toString
    )

    // -----------------------------------------------------------------------
    // 3. Create transaction dataset
    // -----------------------------------------------------------------------

    val transactionDf: DataFrame =
      spark.range(0, transactionCount)
        .select(
          col("id").alias("transaction_id"),

          // Map transactions to customers.
          (col("id") % customerCount).alias("customer_id"),

          // Deterministic transaction amount.
          //
          // The actual monetary value is not important for this experiment.
          // What matters is that multiple transactions share the same
          // customer_id, making partial aggregation meaningful.
          (col("id") % 1000).alias("amount")
        )

    // -----------------------------------------------------------------------
    // 4. Define aggregation
    // -----------------------------------------------------------------------

    val aggregatedDf: DataFrame =
      transactionDf
        .groupBy(col("customer_id"))
        .agg(
          sum(col("amount")).alias("total_amount")
        )

    // -----------------------------------------------------------------------
    // 5. Print experiment configuration
    // -----------------------------------------------------------------------

    println()
    println("====================================================")
    println("MODULE 1.7.1 - BASELINE AGGREGATION")
    println("====================================================")

    println()
    println("EXPERIMENT CONFIGURATION")
    println("----------------------------------------------------")

    println(s"Transactions = $transactionCount")
    println(s"Customers = $customerCount")
    println(s"Shuffle partitions = $shufflePartitions")
    println(s"UI pause = $uiPauseSeconds seconds")

    // -----------------------------------------------------------------------
    // 6. Inspect physical plan
    // -----------------------------------------------------------------------

    println()
    println("====================================================")
    println("PHYSICAL PLAN")
    println("====================================================")

    aggregatedDf.explain("formatted")

    // -----------------------------------------------------------------------
    // 7. Execute aggregation
    // -----------------------------------------------------------------------

    println()
    println("====================================================")
    println("EXECUTION")
    println("====================================================")

    val start = System.nanoTime()

    aggregatedDf.foreachPartition { rows: Iterator[Row] =>
      while (rows.hasNext) {
        rows.next()
      }
    }

    val seconds =
      (System.nanoTime() - start) / 1e9

    // -----------------------------------------------------------------------
    // 8. Execution summary
    // -----------------------------------------------------------------------

    println()
    println("====================================================")
    println("EXECUTION SUMMARY")
    println("====================================================")

    println(s"Input rows = $transactionCount")
    println(s"Expected grouping keys = $customerCount")
    println(f"Execution time = $seconds%.3f s")

    // -----------------------------------------------------------------------
    // 9. Spark UI investigation
    // -----------------------------------------------------------------------

    println()
    println("====================================================")
    println("SPARK UI")
    println("====================================================")

    println("http://localhost:4040")

    println()
    println("Inspect aggregation, shuffle and task metrics in the Spark UI.")

    // -----------------------------------------------------------------------
    // 10. Keep application alive for Spark UI investigation
    // -----------------------------------------------------------------------
    //
    // spark.stop() must happen AFTER this pause.
    //
    // Otherwise the SparkContext is stopped and the local Spark UI becomes
    // unavailable immediately after the workload finishes.

    if (uiPauseSeconds > 0) {

      println()
      println("====================================================")
      println("SPARK UI PAUSE")
      println("====================================================")

      println(
        s"Spark UI will remain available for $uiPauseSeconds seconds."
      )

      println(
        "Open http://localhost:4040 and inspect the completed stages."
      )

      Thread.sleep(uiPauseSeconds * 1000L)

      println("UI pause completed.")
    }

    // -----------------------------------------------------------------------
    // 11. Clean shutdown
    // -----------------------------------------------------------------------

    spark.stop()
  }
}
