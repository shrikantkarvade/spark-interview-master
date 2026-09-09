package com.shrikant.spark.joins

import com.shrikant.spark.foundation.EnterpriseSparkSession
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions._

/**
 * Module 1.6.3 — Broadcast Threshold
 *
 * Demonstrates how Spark's autoBroadcastJoinThreshold influences
 * the physical join strategy selected by the optimizer.
 *
 * Experiment goal:
 *
 *   1. Create a large fact table.
 *   2. Create a relatively small customer dimension.
 *   3. Run the same join with different broadcast thresholds.
 *   4. Inspect the physical plan.
 *   5. Observe when Spark chooses Broadcast Hash Join versus
 *      a shuffle-based strategy such as Sort Merge Join.
 *
 * Example:
 *
 *   Threshold = 10 MiB
 *       -> Customer dimension can qualify for broadcast
 *       -> BroadcastHashJoin is expected
 *
 *   Threshold = 1 MiB
 *       -> Customer dimension is above the threshold
 *       -> Broadcast is not selected
 *       -> SortMergeJoin is expected
 *
 * The key engineering lesson is that the broadcast threshold can
 * significantly change the physical execution strategy.
 */
object BroadcastThresholdExperiment {

  def main(args: Array[String]): Unit = {

    // -----------------------------------------------------------------------
    // 1. Parse the broadcast threshold
    // -----------------------------------------------------------------------
    //
    // The threshold is supplied in bytes.
    //
    // Example:
    //
    //   10 MiB = 10 * 1024 * 1024 bytes
    //   1 MiB  =  1 * 1024 * 1024 bytes
    //
    // Passing the threshold as an argument allows us to run the same
    // workload with different broadcast limits.

    val broadcastThreshold: Long =
      if (args.nonEmpty) args(0).toLong
      else 10L * 1024 * 1024

    // -----------------------------------------------------------------------
    // 2. Create the Spark session
    // -----------------------------------------------------------------------

    val spark: SparkSession =
      EnterpriseSparkSession.create(
        "Broadcast Threshold Experiment",
        "dev"
      )

    // -----------------------------------------------------------------------
    // 3. Configure broadcast threshold
    // -----------------------------------------------------------------------
    //
    // Spark uses this configuration to determine whether a relation is
    // small enough to be considered for Broadcast Hash Join.
    //
    // Important:
    //
    // This is a planning threshold. It does not mean that Spark will
    // always broadcast a relation below this size; other optimizer
    // conditions and statistics can also influence the final strategy.

    spark.conf.set(
      "spark.sql.autoBroadcastJoinThreshold",
      broadcastThreshold.toString
    )

    // Keep shuffle partitions fixed so that changing the broadcast
    // threshold is the primary variable in this experiment.
    spark.conf.set(
      "spark.sql.shuffle.partitions",
      "20"
    )

    // -----------------------------------------------------------------------
    // 4. Create the fact dataset
    // -----------------------------------------------------------------------
    //
    // Create 10 million transaction records.
    //
    // This is intentionally the large side of the join.
    //
    // customer_id contains 100,000 distinct values.

    val factDf: DataFrame =
      spark.range(0, 10000000)
        .select(
          col("id").alias("transaction_id"),

          // Map each transaction to one of 100,000 customers.
          (col("id") % 100000).alias("customer_id"),

          // Generate a deterministic transaction amount.
          (col("id") % 1000).alias("amount")
        )

    // -----------------------------------------------------------------------
    // 5. Create the customer dimension
    // -----------------------------------------------------------------------
    //
    // Create 100,000 customer records.
    //
    // This is intentionally much smaller than the fact table.
    //
    // Depending on the configured broadcast threshold, Spark may either:
    //
    //   - broadcast this relation, or
    //   - use a shuffle-based join strategy.

    val customerDf: DataFrame =
      spark.range(0, 100000)
        .select(
          col("id").alias("customer_id"),

          concat(
            lit("CUSTOMER_"),
            col("id")
          ).alias("customer_name")
        )

    // -----------------------------------------------------------------------
    // 6. Define the join
    // -----------------------------------------------------------------------
    //
    // No join hint is used.
    //
    // This is important because the experiment is testing the effect of
    // spark.sql.autoBroadcastJoinThreshold on Spark's natural strategy
    // selection.

    val joinedDf =
      factDf.join(
        customerDf,
        Seq("customer_id"),
        "inner"
      )

    println(
      s"=== Module 1.6.3 — Broadcast Threshold | " +
        s"threshold=$broadcastThreshold bytes ==="
    )

    // -----------------------------------------------------------------------
    // 7. Inspect the physical plan
    // -----------------------------------------------------------------------
    //
    // This is the primary evidence for the experiment.
    //
    // With a sufficiently high threshold, we expect:
    //
    //   BroadcastHashJoin
    //          |
    //   BroadcastExchange
    //
    // With a threshold below the dimension's estimated size, we expect
    // a shuffle-based strategy such as:
    //
    //   SortMergeJoin
    //       /       \
    //   Exchange   Exchange
    //      |          |
    //    Sort       Sort
    //
    // Always validate the actual physical plan instead of assuming
    // which strategy Spark selected.

    println("Physical plan:")
    joinedDf.explain("formatted")

    // -----------------------------------------------------------------------
    // 8. Execute the complete join
    // -----------------------------------------------------------------------
    //
    // Spark transformations are lazy.
    //
    // The join is not executed until an action is called.
    //
    // foreachPartition consumes every result row and therefore triggers
    // execution of the complete join.

    val start = System.nanoTime()

    joinedDf.foreachPartition { rows: Iterator[Row] =>
      while (rows.hasNext) {
        rows.next()
      }
    }

    // Convert nanoseconds to seconds for easier comparison.

    val seconds =
      (System.nanoTime() - start) / 1e9

    // -----------------------------------------------------------------------
    // 9. Print execution summary
    // -----------------------------------------------------------------------

    println("=== Execution Summary ===")
    println("Rows joined = 10,000,000")
    println(f"Execution time = $seconds%.3f s")

    // -----------------------------------------------------------------------
    // 10. Stop Spark
    // -----------------------------------------------------------------------

    spark.stop()
  }
}
