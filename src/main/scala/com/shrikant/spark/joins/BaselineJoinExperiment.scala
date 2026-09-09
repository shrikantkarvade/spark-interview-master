package com.shrikant.spark.joins

import com.shrikant.spark.foundation.EnterpriseSparkSession
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions._

/**
 * Module 1.6.1 — Baseline Join
 *
 * Establishes the baseline Spark join behavior before introducing
 * explicit join optimization techniques such as:
 *
 *   - Broadcast Hash Join
 *   - Sort Merge Join
 *   - Shuffle Hash Join
 *   - Join hints
 *   - AQE join conversion
 *   - AQE skew handling
 *
 * Experiment goal:
 *
 *   1. Create a large fact table.
 *   2. Create a customer dimension table.
 *   3. Join both datasets on customer_id.
 *   4. Allow Spark to choose the join strategy naturally.
 *   5. Inspect the physical plan.
 *   6. Execute the complete join and measure execution time.
 *
 * The physical plan is the primary evidence used to understand which
 * join strategy Spark selected.
 */
object BaselineJoinExperiment {

  def main(args: Array[String]): Unit = {

    // -----------------------------------------------------------------------
    // 1. Create the Spark session
    // -----------------------------------------------------------------------
    //
    // EnterpriseSparkSession centralizes the Spark configuration used by
    // the project and provides the "dev" execution environment.

    val spark: SparkSession =
      EnterpriseSparkSession.create(
        "Baseline Join Experiment",
        "dev"
      )

    // -----------------------------------------------------------------------
    // 2. Create the fact dataset
    // -----------------------------------------------------------------------
    //
    // Create 10 million transaction records.
    //
    // customer_id has 100,000 distinct values, so each customer has
    // approximately 100 transactions on average.
    //
    // This represents the typical pattern of:
    //
    //              Large Fact Table
    //                     |
    //                     | customer_id
    //                     v
    //              Customer Dimension
    //
    // The fact table is intentionally much larger than the dimension.

    val factDf: DataFrame =
      spark.range(0, 10000000)
        .select(
          col("id").alias("transaction_id"),

          // Map every transaction to one of 100,000 customers.
          (col("id") % 100000).alias("customer_id"),

          // Generate a simple deterministic transaction amount.
          (col("id") % 1000).alias("amount")
        )

    // -----------------------------------------------------------------------
    // 3. Create the customer dimension
    // -----------------------------------------------------------------------
    //
    // Create 100,000 customer records.
    //
    // This dataset is intentionally much smaller than the fact table.
    // Spark may therefore consider it a candidate for Broadcast Hash Join,
    // depending on its runtime statistics and broadcast threshold.

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
    // No join hint is provided.
    //
    // This is important for the baseline experiment because we want Spark's
    // optimizer to choose the physical join strategy rather than forcing:
    //
    //   BROADCAST
    //   MERGE
    //   SHUFFLE_HASH
    //
    // The physical plan will show which strategy Spark selected.

    val joinedDf =
      factDf.join(
        customerDf,
        Seq("customer_id"),
        "inner"
      )

    println("=== Module 1.6.1 — Baseline Join Experiment ===")

    // -----------------------------------------------------------------------
    // 5. Inspect the physical plan
    // -----------------------------------------------------------------------
    //
    // explain("formatted") provides a readable representation of Spark's
    // physical execution plan.
    //
    // Depending on Spark's statistics and configuration, the plan may
    // contain a BroadcastHashJoin, SortMergeJoin, or another strategy.
    //
    // The physical plan is more useful than assuming which strategy Spark
    // will choose.

    println("Physical plan:")
    joinedDf.explain("formatted")

    // -----------------------------------------------------------------------
    // 6. Execute the complete join
    // -----------------------------------------------------------------------
    //
    // Spark transformations are lazy.
    //
    // Creating joinedDf above does NOT execute the join.
    //
    // foreachPartition is the terminal action that triggers execution.
    // We explicitly consume every row so the complete join must execute.

    val start = System.nanoTime()

    joinedDf.foreachPartition { rows: Iterator[Row] =>
      while (rows.hasNext) {
        rows.next()
      }
    }

    // Convert nanoseconds to seconds for easier interpretation.
    val seconds =
      (System.nanoTime() - start) / 1e9

    // -----------------------------------------------------------------------
    // 7. Print the execution summary
    // -----------------------------------------------------------------------

    println("=== Execution Summary ===")
    println("Rows joined = 10,000,000")
    println(f"Execution time = $seconds%.3f s")

    // -----------------------------------------------------------------------
    // 8. Stop Spark
    // -----------------------------------------------------------------------
    //
    // Explicitly stop the Spark session so resources are released cleanly.

    spark.stop()
  }
}
