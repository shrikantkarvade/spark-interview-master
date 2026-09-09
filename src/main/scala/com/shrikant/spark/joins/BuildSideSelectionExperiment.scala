package com.shrikant.spark.joins

import com.shrikant.spark.foundation.EnterpriseSparkSession
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions._

/**
 * Module 1.6.7 — Build Side Selection
 *
 * Demonstrates how Spark selects the build side of a hash-based join.
 *
 * The experiment compares:
 *
 *   1. Join order = NORMAL
 *      factDf.join(customerDf)
 *
 *   2. Join order = REVERSE
 *      customerDf.join(factDf)
 *
 * The logical join order can change the physical left/right position
 * of the relations. Spark can still select the smaller suitable
 * relation as the build side.
 *
 * Experiment strategies:
 *
 *   SHUFFLE_HASH
 *       -> ShuffledHashJoin
 *
 *   BROADCAST
 *       -> BroadcastHashJoin
 *
 * Expected observations:
 *
 *   SHUFFLE_HASH + NORMAL
 *       -> BuildRight
 *       -> Customer dimension is the build relation
 *
 *   SHUFFLE_HASH + REVERSE
 *       -> BuildLeft
 *       -> Customer dimension is still the build relation
 *
 *   BROADCAST + NORMAL
 *       -> BuildRight
 *       -> Customer dimension is broadcast
 *
 *   BROADCAST + REVERSE
 *       -> BuildLeft
 *       -> Customer dimension is still broadcast
 *
 * Important:
 *
 * BuildLeft / BuildRight describes the physical position of the
 * build relation in the join. It does NOT mean "fact table" or
 * "customer table" intrinsically.
 *
 * Always inspect the physical plan to determine which dataset
 * Spark actually selected as the build side.
 */
object BuildSideSelectionExperiment {

  def main(args: Array[String]): Unit = {

    // -----------------------------------------------------------------------
    // 1. Parse experiment parameters
    // -----------------------------------------------------------------------

    // Broadcast threshold in bytes.
    //
    // For the SHUFFLE_HASH strategy this threshold is not the primary
    // strategy selector, but it is retained as a common experiment
    // parameter across the join experiments.
    val broadcastThreshold: Long =
      if (args.nonEmpty) args(0).toLong
      else 1024L * 1024

    // Number of shuffle partitions.
    val shufflePartitions: Int =
      if (args.length > 1) args(1).toInt
      else 20

    // Join strategy to demonstrate.
    //
    // Supported values:
    //
    //   SHUFFLE_HASH
    //   BROADCAST
    //
    val strategy: String =
      if (args.length > 2) args(2).toUpperCase
      else "SHUFFLE_HASH"

    // Logical join order.
    //
    // NORMAL:
    //   factDf.join(customerDf)
    //
    // REVERSE:
    //   customerDf.join(factDf)
    //
    val joinOrder: String =
      if (args.length > 3) args(3).toUpperCase
      else "NORMAL"

    // -----------------------------------------------------------------------
    // 2. Create the Spark session
    // -----------------------------------------------------------------------

    val spark: SparkSession =
      EnterpriseSparkSession.create(
        "Build Side Selection Experiment",
        "dev"
      )

    // -----------------------------------------------------------------------
    // 3. Configure Spark
    // -----------------------------------------------------------------------

    // Configure the broadcast threshold.
    spark.conf.set(
      "spark.sql.autoBroadcastJoinThreshold",
      broadcastThreshold.toString
    )

    // Keep the shuffle partition count configurable so that the same
    // experiment can be repeated with different shuffle configurations.
    spark.conf.set(
      "spark.sql.shuffle.partitions",
      shufflePartitions.toString
    )

    // -----------------------------------------------------------------------
    // 4. Create the large fact dataset
    // -----------------------------------------------------------------------
    //
    // 10 million transaction records.
    //
    // This represents the large side of the fact-to-dimension join.

    val factDf: DataFrame =
      spark.range(0, 10000000)
        .select(
          col("id").alias("transaction_id"),

          // Map transactions to 100,000 customers.
          (col("id") % 100000).alias("customer_id"),

          // Generate a deterministic amount.
          (col("id") % 1000).alias("amount")
        )

    // -----------------------------------------------------------------------
    // 5. Create the customer dimension
    // -----------------------------------------------------------------------
    //
    // 100,000 customer records.
    //
    // This relation is much smaller than the fact table and is therefore
    // the natural candidate for the build side of a hash-based join.

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
    // 6. Apply the requested join strategy
    // -----------------------------------------------------------------------
    //
    // The strategy determines which physical join implementation we
    // want Spark to consider.
    //
    // We deliberately use hints here because this experiment is about
    // understanding build-side selection within a known join strategy.

    val factWithHint: DataFrame =
      if (strategy == "SHUFFLE_HASH") {
        factDf.hint("SHUFFLE_HASH")
      } else {
        factDf.hint("BROADCAST")
      }

    val customerWithHint: DataFrame =
      if (strategy == "SHUFFLE_HASH") {
        customerDf.hint("SHUFFLE_HASH")
      } else {
        customerDf.hint("BROADCAST")
      }

    // -----------------------------------------------------------------------
    // 7. Control logical join order
    // -----------------------------------------------------------------------
    //
    // NORMAL:
    //
    //   factDf
    //       |
    //       +---- join ---- customerDf
    //
    // REVERSE:
    //
    //   customerDf
    //       |
    //       +---- join ---- factDf
    //
    // The purpose is to observe whether changing the logical order changes
    // the physical BuildLeft / BuildRight designation.
    //
    // The important question is:
    //
    //   Does Spark still select the smaller customer relation as the
    //   build relation even when it appears on the opposite side?

    val joinedDf =
      if (joinOrder == "REVERSE") {

        customerWithHint.join(
          factWithHint,
          Seq("customer_id"),
          "inner"
        )

      } else {

        factWithHint.join(
          customerWithHint,
          Seq("customer_id"),
          "inner"
        )
      }

    println(
      s"=== Module 1.6.7 — Build Side Selection | " +
        s"strategy=$strategy | " +
        s"order=$joinOrder ==="
    )

    // -----------------------------------------------------------------------
    // 8. Inspect the physical plan
    // -----------------------------------------------------------------------
    //
    // This is the most important part of the experiment.
    //
    // Look for:
    //
    //   BroadcastHashJoin ... BuildLeft
    //   BroadcastHashJoin ... BuildRight
    //
    // or:
    //
    //   ShuffledHashJoin ... BuildLeft
    //   ShuffledHashJoin ... BuildRight
    //
    // BuildLeft / BuildRight refers to the physical position of the
    // build relation, not necessarily to the original DataFrame variable.
    //
    // Example:
    //
    //   fact.join(customer)
    //       -> BuildRight
    //
    //   customer.join(fact)
    //       -> BuildLeft
    //
    // In both cases, Spark can still choose customerDf as the actual
    // build relation.

    println("Physical plan:")
    joinedDf.explain("formatted")

    // -----------------------------------------------------------------------
    // 9. Execute the complete join
    // -----------------------------------------------------------------------
    //
    // Spark transformations are lazy.
    //
    // The join is only executed when an action is triggered.
    //
    // foreachPartition consumes every result row so the complete join
    // workload is executed.

    val start = System.nanoTime()

    joinedDf.foreachPartition { rows: Iterator[Row] =>
      while (rows.hasNext) {
        rows.next()
      }
    }

    val seconds =
      (System.nanoTime() - start) / 1e9

    // -----------------------------------------------------------------------
    // 10. Print execution summary
    // -----------------------------------------------------------------------

    println("=== Execution Summary ===")
    println(s"Strategy = $strategy")
    println(s"Join order = $joinOrder")
    println(s"Shuffle partitions = $shufflePartitions")
    println(s"Rows joined = 10,000,000")
    println(f"Execution time = $seconds%.3f s")

    // -----------------------------------------------------------------------
    // 11. Stop Spark
    // -----------------------------------------------------------------------

    spark.stop()
  }
}
