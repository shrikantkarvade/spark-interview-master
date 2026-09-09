package com.shrikant.spark.joins

import com.shrikant.spark.foundation.EnterpriseSparkSession
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions._

/**
 * Module 1.6.2 — Broadcast Hash Join
 *
 * Demonstrates Spark's Broadcast Hash Join (BHJ) for a large fact table
 * joined with a relatively small dimension table.
 *
 * Experiment goal:
 *
 *   1. Create a 10-million-row fact table.
 *   2. Create a 100,000-row customer dimension.
 *   3. Allow Spark to identify the small side as a broadcast candidate.
 *   4. Inspect the physical plan for BroadcastHashJoin.
 *   5. Execute the complete join and measure execution time.
 *
 * Expected physical plan:
 *
 *   BroadcastHashJoin
 *       /       \
 *    Fact       Broadcast
 *               Customer
 *
 * The key optimization is that the customer dimension is broadcast to
 * executors, avoiding a shuffle of the large fact table for the join.
 *
 * Note:
 * This experiment does not use a BROADCAST hint. Spark is expected to
 * select Broadcast Hash Join naturally because the customer dimension
 * is small enough to fit within the configured broadcast threshold.
 */
object BroadcastHashJoinExperiment {

  def main(args: Array[String]): Unit = {

    // -----------------------------------------------------------------------
    // 1. Create the Spark session
    // -----------------------------------------------------------------------
    //
    // Use the project's shared EnterpriseSparkSession so that the
    // experiment runs with the standard project Spark configuration.

    val spark: SparkSession =
      EnterpriseSparkSession.create(
        "Module 1.6.2 — Broadcast Hash Join",
        "dev"
      )

    // -----------------------------------------------------------------------
    // 2. Create the fact dataset
    // -----------------------------------------------------------------------
    //
    // Create 10 million transaction records.
    //
    // customer_id contains 100,000 distinct values, giving approximately
    // 100 transactions per customer.
    //
    // This represents the large side of a typical fact-to-dimension join.

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
    // 3. Create the customer dimension
    // -----------------------------------------------------------------------
    //
    // Create 100,000 customer records.
    //
    // This dataset is intentionally much smaller than the fact table.
    // Spark can therefore consider it for broadcasting.
    //
    // With the default 10 MiB broadcast threshold used in this experiment,
    // this dimension is expected to qualify for Broadcast Hash Join.

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
    // 4. Define the join
    // -----------------------------------------------------------------------
    //
    // No BROADCAST hint is used.
    //
    // This allows Spark's optimizer to make the join-strategy decision
    // based on the available statistics and broadcast threshold.
    //
    // Expected strategy:
    //
    //   BroadcastHashJoin
    //
    // with customerDf used as the build/broadcast side.

    val joinedDf =
      factDf.join(
        customerDf,
        Seq("customer_id"),
        "inner"
      )

    println("=== Module 1.6.2 — Broadcast Hash Join ===")

    // -----------------------------------------------------------------------
    // 5. Inspect the physical plan
    // -----------------------------------------------------------------------
    //
    // explain("formatted") allows us to verify whether Spark selected:
    //
    //   BroadcastHashJoin
    //   BroadcastExchange
    //
    // The absence of a join-side Exchange for the fact table is important.
    //
    // Instead of shuffling the large fact dataset, Spark broadcasts the
    // smaller customer dataset.

    println("Physical plan:")
    joinedDf.explain("formatted")

    // -----------------------------------------------------------------------
    // 6. Execute the complete join
    // -----------------------------------------------------------------------
    //
    // Spark transformations are lazy. The join is not executed when
    // joinedDf is created.
    //
    // foreachPartition triggers execution and consumes every result row.
    //
    // Explicitly consuming the iterator ensures that the complete join
    // workload is executed.

    val start = System.nanoTime()

    joinedDf.foreachPartition { rows: Iterator[Row] =>
      while (rows.hasNext) {
        rows.next()
      }
    }

    // Convert nanoseconds to seconds for easier comparison between
    // experiments.

    val seconds =
      (System.nanoTime() - start) / 1e9

    // -----------------------------------------------------------------------
    // 7. Print execution summary
    // -----------------------------------------------------------------------

    println("=== Execution Summary ===")
    println("Rows joined = 10,000,000")
    println(f"Execution time = $seconds%.3f s")

    // -----------------------------------------------------------------------
    // 8. Stop Spark
    // -----------------------------------------------------------------------
    //
    // Release the Spark session and associated resources.

    spark.stop()
  }
}
