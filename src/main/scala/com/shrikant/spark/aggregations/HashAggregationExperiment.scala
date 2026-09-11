package com.shrikant.spark.aggregations

import com.shrikant.spark.foundation.EnterpriseSparkSession
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions._

/**
 * Module 1.7.2 — Hash Aggregation
 *
 * Investigates Spark's HashAggregate execution strategy for a large
 * groupBy + SUM workload.
 *
 * Workload:
 *
 *   Transactions:
 *     - 10,000,000 rows
 *     - 100,000 customers
 *
 *   Aggregation:
 *
 *     SUM(amount) GROUP BY customer_id
 *
 * Expected physical execution:
 *
 *   HashAggregate [partial_sum]
 *       ↓
 *   Exchange [hashpartitioning(customer_id)]
 *       ↓
 *   HashAggregate [sum]
 *
 * Engineering questions:
 *
 *   1. Why does Spark choose HashAggregate for this workload?
 *   2. What happens during partial aggregation?
 *   3. How many rows are produced before the shuffle?
 *   4. How much memory does HashAggregate use?
 *   5. Does the aggregation spill to memory or disk?
 *   6. Does Spark perform sort fallback?
 *   7. How does the physical plan correlate with Spark UI metrics?
 *
 * This experiment intentionally keeps the workload similar to Module 1.7.1.
 * The purpose is to investigate HashAggregate behavior without introducing
 * unrelated workload changes.
 *
 * Important:
 *
 *   Configuration values are not treated as proof of execution behavior.
 *   The physical plan and Spark UI are the source of truth.
 *
 * Optional argument:
 *
 *   --ui-pause=<seconds>
 *
 * Example:
 *
 *   --ui-pause=300
 *
 * Keeps the Spark application alive after execution so that the Spark UI
 * remains available for runtime investigation.
 */
object HashAggregationExperiment {

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

    // Optional pause used to keep the local Spark UI available after the
    // workload finishes.
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
        "Hash Aggregation Experiment",
        "dev"
      )

    // Keep shuffle parallelism explicit so that the experiment remains
    // reproducible and can later be compared with different partition
    // configurations in Module 1.7.5.
    spark.conf.set(
      "spark.sql.shuffle.partitions",
      shufflePartitions.toString
    )

    // -----------------------------------------------------------------------
    // 3. Print experiment configuration
    // -----------------------------------------------------------------------

    println()
    println("====================================================")
    println("MODULE 1.7.2 - HASH AGGREGATION")
    println("====================================================")

    println()
    println("EXPERIMENT CONFIGURATION")
    println("----------------------------------------------------")

    println(s"Transactions = $transactionCount")
    println(s"Customers = $customerCount")
    println(s"Shuffle partitions = $shufflePartitions")
    println(s"UI pause = $uiPauseSeconds seconds")

    // -----------------------------------------------------------------------
    // 4. Print relevant Spark aggregation configuration
    // -----------------------------------------------------------------------
    //
    // These settings help explain the environment in which HashAggregate
    // executes. They are diagnostic information, not proof that a particular
    // execution path was used.
    //
    // The physical plan and Spark UI remain the authoritative evidence.

    println()
    println("SPARK AGGREGATION CONFIGURATION")
    println("----------------------------------------------------")

    println(
      s"spark.sql.adaptive.enabled = " +
        spark.conf.get("spark.sql.adaptive.enabled")
    )

    println(
      s"spark.sql.shuffle.partitions = " +
        spark.conf.get("spark.sql.shuffle.partitions")
    )

    println(
      s"spark.sql.codegen.aggregate.map.twolevel.enabled = " +
        spark.conf.get(
          "spark.sql.codegen.aggregate.map.twolevel.enabled"
        )
    )

    println(
      s"spark.sql.codegen.aggregate.map.twolevel.partialOnly = " +
        spark.conf.get(
          "spark.sql.codegen.aggregate.map.twolevel.partialOnly"
        )
    )

    // -----------------------------------------------------------------------
    // 5. Create transaction dataset
    // -----------------------------------------------------------------------
    //
    // The dataset is deterministic so repeated runs use the same logical
    // workload.
    //
    // Each customer receives multiple transactions. This creates duplicate
    // grouping keys within each input partition and allows the partial
    // HashAggregate to combine rows before the shuffle.

    val transactionDf: DataFrame =
      spark.range(0, transactionCount)
        .select(
          col("id").alias("transaction_id"),

          // Deterministically assign each transaction to a customer.
          (col("id") % customerCount).alias("customer_id"),

          // Deterministic transaction amount.
          //
          // The exact monetary distribution is not the focus of this
          // experiment. The important property is repeated customer keys.
          (col("id") % 1000).alias("amount")
        )

    // -----------------------------------------------------------------------
    // 6. Define HashAggregate workload
    // -----------------------------------------------------------------------
    //
    // groupBy + SUM allows Spark's physical planner to select an aggregation
    // implementation.
    //
    // For this workload, we expect HashAggregate rather than a
    // sort-based aggregation path.
    //
    // The expected two-phase execution is:
    //
    //   partial HashAggregate
    //           ↓
    //        Exchange
    //           ↓
    //   final HashAggregate
    //
    // The partial aggregation reduces duplicate customer rows locally
    // before they enter the shuffle.

    val aggregatedDf: DataFrame =
      transactionDf
        .groupBy(col("customer_id"))
        .agg(
          sum(col("amount")).alias("total_amount")
        )

    // -----------------------------------------------------------------------
    // 7. Inspect physical plan before execution
    // -----------------------------------------------------------------------
    //
    // explain("formatted") exposes the physical operators selected by Spark.
    //
    // Look specifically for:
    //
    //   HashAggregate [partial_sum]
    //       ↓
    //   Exchange
    //       ↓
    //   HashAggregate [sum]
    //
    // An AdaptiveSparkPlan may appear because AQE is enabled.
    //
    // Before execution, the adaptive plan may not yet contain the final
    // runtime-adjusted execution decisions.

    println()
    println("====================================================")
    println("INITIAL PHYSICAL PLAN")
    println("====================================================")

    aggregatedDf.explain("formatted")

    // -----------------------------------------------------------------------
    // 8. Execute the aggregation
    // -----------------------------------------------------------------------
    //
    // Spark transformations are lazy.
    //
    // The aggregation is not executed by defining aggregatedDf or calling
    // explain().
    //
    // foreachPartition is the terminal action that forces the complete
    // aggregation to execute.
    //
    // We intentionally consume the rows without printing them. Printing
    // 100,000 result rows would introduce unnecessary console I/O and would
    // distort the experiment.

    println()
    println("====================================================")
    println("EXECUTION")
    println("====================================================")

    val startTimeNanos = System.nanoTime()

    aggregatedDf.foreachPartition { rows: Iterator[Row] =>
      while (rows.hasNext) {
        rows.next()
      }
    }

    val executionSeconds =
      (System.nanoTime() - startTimeNanos) / 1e9

    // -----------------------------------------------------------------------
    // 9. Print post-execution physical plan
    // -----------------------------------------------------------------------
    //
    // This is particularly important when AQE is enabled.
    //
    // The initial plan represents Spark's original physical strategy.
    //
    // The executed plan can contain runtime adaptations such as:
    //
    //   - AQEShuffleRead
    //   - coalesced shuffle partitions
    //   - adaptive join changes
    //   - skew handling
    //
    // For this experiment, we want to determine whether the aggregation
    // remains HashAggregate during actual execution.

    println()
    println("====================================================")
    println("EXECUTED PHYSICAL PLAN")
    println("====================================================")

    println(
      aggregatedDf.queryExecution.executedPlan.toString
    )

    // -----------------------------------------------------------------------
    // 10. Execution summary
    // -----------------------------------------------------------------------

    println()
    println("====================================================")
    println("EXECUTION SUMMARY")
    println("====================================================")

    println(s"Input rows = $transactionCount")
    println(s"Expected grouping keys = $customerCount")

    println(
      f"Application action time = $executionSeconds%.3f s"
    )

    // -----------------------------------------------------------------------
    // 11. Spark UI investigation
    // -----------------------------------------------------------------------
    //
    // The Spark UI provides runtime evidence that cannot be obtained reliably
    // from the physical plan alone.
    //
    // Inspect:
    //
    //   SQL tab:
    //     - Query duration
    //     - operator metrics
    //     - HashAggregate output rows
    //     - aggregation build time
    //     - peak memory
    //     - spill size
    //     - sort fallback tasks
    //     - average hash probes per key
    //
    //   Stages tab:
    //     - task duration
    //     - shuffle write
    //     - shuffle read
    //     - records written/read
    //     - spill memory
    //     - spill disk
    //
    // These metrics allow us to connect:
    //
    //   Physical operator
    //          ↓
    //   Runtime behavior
    //          ↓
    //   Resource consumption

    println()
    println("====================================================")
    println("SPARK UI")
    println("====================================================")

    println("http://localhost:4040")

    println()
    println(
      "Inspect HashAggregate, shuffle, memory and spill metrics."
    )

    // -----------------------------------------------------------------------
    // 12. Keep application alive for UI investigation
    // -----------------------------------------------------------------------
    //
    // spark.stop() must occur AFTER this pause.
    //
    // Otherwise the SparkContext is stopped and the local Spark UI becomes
    // unavailable immediately after the workload completes.

    if (uiPauseSeconds > 0) {

      println()
      println("====================================================")
      println("SPARK UI PAUSE")
      println("====================================================")

      println(
        s"Spark UI will remain available for $uiPauseSeconds seconds."
      )

      println(
        "Open http://localhost:4040 and inspect the completed query."
      )

      Thread.sleep(uiPauseSeconds * 1000L)

      println("UI pause completed.")
    }

    // -----------------------------------------------------------------------
    // 13. Clean shutdown
    // -----------------------------------------------------------------------

    spark.stop()
  }
}
