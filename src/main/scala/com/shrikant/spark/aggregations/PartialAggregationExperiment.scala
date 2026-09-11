package com.shrikant.spark.aggregations

import com.shrikant.spark.foundation.EnterpriseSparkSession
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions._

/**
 * Module 1.7.3 — Partial Aggregation
 *
 * Investigates how grouping-key cardinality affects Spark's ability to
 * perform partial aggregation before the shuffle.
 *
 * ---------------------------------------------------------------------------
 * Experiment objective
 * ---------------------------------------------------------------------------
 *
 * Spark's aggregation normally uses a two-phase execution model:
 *
 * Input rows
 * ↓
 * Partial HashAggregate
 * ↓
 * Exchange
 * ↓
 * Final HashAggregate
 *
 * Partial aggregation can dramatically reduce shuffle volume when multiple
 * input rows with the same grouping key are processed within the same
 * upstream partition.
 *
 * This experiment quantifies that reduction by keeping the input row count
 * constant while changing the number of distinct grouping keys.
 *
 * Example:
 *
 * 10M input rows / 100K customers
 * → many duplicate keys
 * → strong local aggregation
 * → relatively small shuffle
 *
 * 10M input rows / 10M customers
 * → mostly unique keys
 * → little local aggregation
 * → much larger shuffle
 *
 * ---------------------------------------------------------------------------
 * Engineering questions
 * ---------------------------------------------------------------------------
 *
 *   1. How much can partial aggregation reduce shuffle records?
 *      2. How does grouping-key cardinality affect that reduction?
 *      3. Why does the same 10M-row input produce different shuffle volumes?
 *      4. When does partial aggregation provide little benefit?
 *      5. Why is grouping-key cardinality an important production consideration?
 *
 * ---------------------------------------------------------------------------
 * Important interpretation rule
 * ---------------------------------------------------------------------------
 *
 * Partial aggregation is performed independently inside each upstream
 * partition.
 *
 * Therefore, the number of partial aggregate rows is not necessarily equal
 * to the number of globally distinct grouping keys.
 *
 * If the same grouping key occurs in multiple input partitions, each partition
 * can produce its own partial aggregate for that key. The final aggregation
 * combines those partial results after the shuffle.
 *
 * The physical plan and Spark UI are the source of truth for execution.
 *
 * ---------------------------------------------------------------------------
 * Optional argument
 * ---------------------------------------------------------------------------
 *
 * --ui-pause=<seconds>
 *
 * Example:
 *
 * --ui-pause=300
 *
 * Keeps the Spark application alive after execution so that the Spark UI
 * remains available for manual investigation.
 */
object PartialAggregationExperiment {

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

    val uiPauseSeconds: Int =
      args
        .find(_.startsWith("--ui-pause="))
        .map(_.stripPrefix("--ui-pause=").toInt)
        .getOrElse(0)

    // -----------------------------------------------------------------------
    // 2. Validate parameters
    // -----------------------------------------------------------------------
    //
    // Invalid cardinality or partition values would make the experiment
    // misleading or cause a runtime failure. Fail early with a clear message.

    require(
      transactionCount > 0,
      "transactionCount must be greater than zero."
    )

    require(
      customerCount > 0,
      "customerCount must be greater than zero."
    )

    require(
      shufflePartitions > 0,
      "shufflePartitions must be greater than zero."
    )

    require(
      customerCount <= transactionCount,
      "customerCount should not exceed transactionCount for this experiment."
    )

    require(
      uiPauseSeconds >= 0,
      "uiPauseSeconds cannot be negative."
    )

    // -----------------------------------------------------------------------
    // 3. Create Spark session
    // -----------------------------------------------------------------------

    val spark: SparkSession =
      EnterpriseSparkSession.create(
        "Partial Aggregation Experiment",
        "dev"
      )

    // Keep shuffle parallelism explicit so that cardinality is the primary
    // experimental variable rather than an accidental change in shuffle
    // configuration.
    spark.conf.set(
      "spark.sql.shuffle.partitions",
      shufflePartitions.toString
    )

    // -----------------------------------------------------------------------
    // 4. Print experiment configuration
    // -----------------------------------------------------------------------

    println()
    println("====================================================")
    println("MODULE 1.7.3 - PARTIAL AGGREGATION")
    println("====================================================")

    println()
    println("EXPERIMENT CONFIGURATION")
    println("----------------------------------------------------")

    println(s"Transactions = $transactionCount")
    println(s"Customers / grouping keys = $customerCount")
    println(s"Shuffle partitions = $shufflePartitions")
    println(s"UI pause = $uiPauseSeconds seconds")

    val averageTransactionsPerKey =
      transactionCount.toDouble / customerCount.toDouble

    println(
      f"Average transactions per grouping key = " +
        f"$averageTransactionsPerKey%.2f"
    )

    // -----------------------------------------------------------------------
    // 5. Print relevant Spark configuration
    // -----------------------------------------------------------------------
    //
    // These values describe the execution environment.
    //
    // They are diagnostic information only. They do not prove that a
    // particular physical execution path was used.

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
    // 6. Create deterministic transaction dataset
    // -----------------------------------------------------------------------
    //
    // The transaction count remains constant across runs.
    //
    // customerCount controls grouping-key cardinality and is therefore the
    // primary experimental variable.
    //
    // Example:
    //
    //   customerCount = 100,000
    //
    // gives approximately 100 transactions per customer.
    //
    // Increasing customerCount reduces the number of duplicate grouping keys
    // available for local combination.

    val transactionDf: DataFrame =
      spark.range(0, transactionCount)
        .select(
          col("id").alias("transaction_id"),

          // Map each transaction deterministically to a grouping key.
          //
          // The modulo operation makes the number of distinct grouping keys
          // directly controllable through customerCount.
          (col("id") % customerCount).alias("customer_id"),

          // Deterministic aggregation value.
          (col("id") % 1000).alias("amount")
        )

    // -----------------------------------------------------------------------
    // 7. Define aggregation
    // -----------------------------------------------------------------------
    //
    // The logical operation is intentionally simple:
    //
    //   SUM(amount) GROUP BY customer_id
    //
    // We do not add ordering, joins, filters or explicit hints because those
    // would introduce additional execution variables.
    //
    // The experiment is specifically about the relationship between:
    //
    //   grouping-key cardinality
    //             ↓
    //   partial aggregation effectiveness
    //             ↓
    //   shuffle volume

    val aggregatedDf: DataFrame =
      transactionDf
        .groupBy(col("customer_id"))
        .agg(
          sum(col("amount")).alias("total_amount")
        )

    // -----------------------------------------------------------------------
    // 8. Inspect initial physical plan
    // -----------------------------------------------------------------------
    //
    // We expect Spark to select:
    //
    //   HashAggregate [partial_sum]
    //        ↓
    //   Exchange
    //        ↓
    //   HashAggregate [sum]
    //
    // The partial_sum operator is the critical operator for this experiment.
    //
    // It combines rows locally before the Exchange.
    //
    // The Spark UI will tell us how many rows actually leave that operator.

    println()
    println("====================================================")
    println("INITIAL PHYSICAL PLAN")
    println("====================================================")

    aggregatedDf.explain("formatted")

    // -----------------------------------------------------------------------
    // 9. Execute aggregation
    // -----------------------------------------------------------------------
    //
    // Spark transformations are lazy.
    //
    // foreachPartition forces the complete aggregation to execute.
    //
    // We consume the result without printing it because console output would
    // introduce unnecessary I/O and distort the benchmark.

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
    // 10. Print executed physical plan
    // -----------------------------------------------------------------------
    //
    // AQE may modify the runtime representation of the initial plan.
    //
    // We therefore inspect the executed plan after the action has completed.
    //
    // In particular, look for:
    //
    //   AQEShuffleRead
    //
    // and:
    //
    //   coalesced
    //
    // This allows us to distinguish the configured shuffle partition count
    // from the number of partitions actually consumed at runtime.

    println()
    println("====================================================")
    println("EXECUTED PHYSICAL PLAN")
    println("====================================================")

    println(
      aggregatedDf.queryExecution.executedPlan.toString
    )

    // -----------------------------------------------------------------------
    // 11. Calculate theoretical comparison metrics
    // -----------------------------------------------------------------------
    //
    // These are workload-level reference values, not measured Spark UI
    // metrics.
    //
    // The actual partial aggregate output and shuffle record count must be
    // taken from the Spark UI.
    //
    // For this deterministic workload, the global number of grouping keys
    // should be customerCount.

    println()
    println("====================================================")
    println("WORKLOAD SUMMARY")
    println("====================================================")

    println(s"Input rows = $transactionCount")
    println(s"Global grouping keys = $customerCount")

    println(
      f"Average rows per grouping key = " +
        f"$averageTransactionsPerKey%.2f"
    )

    println(
      f"Application action time = $executionSeconds%.3f s"
    )

    // -----------------------------------------------------------------------
    // 12. Spark UI investigation
    // -----------------------------------------------------------------------
    //
    // This is the most important section of the experiment.
    //
    // In SQL → Query details, capture:
    //
    //   Partial HashAggregate:
    //     - number of output rows
    //     - time in aggregation build
    //     - peak memory
    //     - spill size
    //     - sort fallback tasks
    //     - average hash probes
    //
    //   Exchange:
    //     - shuffle records written
    //     - shuffle bytes written
    //
    //   AQEShuffleRead:
    //     - original partition count
    //     - coalesced partition count
    //     - partition data size
    //
    //   Final HashAggregate:
    //     - number of output rows
    //     - aggregation build time
    //
    // The critical calculation is:
    //
    //   reduction =
    //     1 - (shuffle records written / input records)
    //
    // For the 100K-customer baseline:
    //
    //   1 - (200K / 10M) = 98%
    //
    // The actual value for each cardinality must be derived from the UI,
    // rather than assumed from the workload definition.

    println()
    println("====================================================")
    println("SPARK UI INVESTIGATION")
    println("====================================================")

    println("http://localhost:4040")

    println()
    println(
      "Capture partial aggregate output rows and shuffle records written."
    )

    println(
      "Calculate shuffle reduction using actual Spark UI metrics."
    )

    // -----------------------------------------------------------------------
    // 13. Keep application alive for Spark UI investigation
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
        "Open http://localhost:4040 and inspect Query 0."
      )

      Thread.sleep(uiPauseSeconds * 1000L)

      println("UI pause completed.")
    }

    // -----------------------------------------------------------------------
    // 14. Clean shutdown
    // -----------------------------------------------------------------------

    spark.stop()
  }
}
