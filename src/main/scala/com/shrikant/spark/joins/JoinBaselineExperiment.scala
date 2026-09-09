package com.shrikant.spark.joins

import com.shrikant.spark.foundation.EnterpriseSparkSession
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
 * Module 1.6.1 — Baseline Join
 *
 * Establishes the baseline execution behavior for a large fact-to-dimension
 * join before introducing join-specific optimizations.
 *
 * Workload:
 *
 *   Orders:
 *     - 10,000,000 rows
 *     - 1,000,000 distinct customer IDs
 *
 *   Customers:
 *     - 1,000,000 rows
 *     - One row per customer
 *
 * Join:
 *
 *   orders.customer_id = customers.customer_id
 *
 * Experiment controls:
 *
 *   spark.sql.autoBroadcastJoinThreshold = -1
 *       Broadcasting is disabled so that Spark cannot choose a
 *       BroadcastHashJoin.
 *
 *   spark.sql.adaptive.enabled = false
 *       AQE is disabled so that the baseline represents the original
 *       physical execution plan without runtime plan changes.
 *
 * Expected physical strategy:
 *
 *   SortMergeJoin
 *       |
 *       +---- Exchange + Sort (orders)
 *       |
 *       +---- Exchange + Sort (customers)
 *
 * The physical plan is the primary evidence for the selected join strategy.
 * Execution time and Spark UI metrics are used as runtime evidence.
 *
 * Engineering lesson:
 *
 * Establish a controlled baseline first. Join optimization should be based
 * on physical-plan evidence and runtime measurements rather than assumptions
 * about which join strategy Spark will choose.
 */
object JoinBaselineExperiment {

  def main(args: Array[String]): Unit = {

    // -----------------------------------------------------------------------
    // 1. Start the experiment
    // -----------------------------------------------------------------------

    println()
    println("====================================================")
    println("MODULE 1.6.1 - BASELINE JOIN")
    println("====================================================")

    val spark: SparkSession =
      EnterpriseSparkSession.create(
        appName = "Module 1.6.1 - Baseline Join",
        environment = "dev"
      )

    // -----------------------------------------------------------------------
    // 2. Configure a controlled baseline
    // -----------------------------------------------------------------------
    //
    // The purpose of these settings is to isolate the baseline join
    // mechanics.
    //
    // Broadcasting is disabled because a broadcast join would avoid the
    // normal join-side shuffle and would make this an optimization rather
    // than a neutral baseline.
    //
    // AQE is disabled because AQE can modify the physical execution plan
    // at runtime. That behavior is studied separately in later experiments.

    spark.conf.set(
      "spark.sql.autoBroadcastJoinThreshold",
      "-1"
    )

    spark.conf.set(
      "spark.sql.adaptive.enabled",
      "false"
    )

    println()
    println("====================================================")
    println("EXPERIMENT CONFIGURATION")
    println("====================================================")

    println(
      s"spark.master = ${spark.sparkContext.master}"
    )

    println(
      "spark.sql.autoBroadcastJoinThreshold = " +
        spark.conf.get("spark.sql.autoBroadcastJoinThreshold")
    )

    println(
      "spark.sql.adaptive.enabled = " +
        spark.conf.get("spark.sql.adaptive.enabled")
    )

    println(
      "spark.sql.shuffle.partitions = " +
        spark.conf.get("spark.sql.shuffle.partitions")
    )

    // -----------------------------------------------------------------------
    // 3. Create the orders dataset
    // -----------------------------------------------------------------------
    //
    // This represents the large fact-side dataset.
    //
    // 10 million rows are distributed across 1 million customers.
    // Multiple orders therefore map to the same customer dimension row.
    //
    // The deterministic expressions make the experiment repeatable.

    val orders =
      spark.range(10000000)
        .select(
          col("id").as("order_id"),

          // 1,000,000 customer IDs.
          (col("id") % 1000000).as("customer_id"),

          // Deterministic amount used as a representative measure.
          (col("id") % 10000)
            .cast("double")
            .as("amount")
        )

    // -----------------------------------------------------------------------
    // 4. Create the customers dataset
    // -----------------------------------------------------------------------
    //
    // This represents the customer dimension.
    //
    // There are 1 million customer records. Unlike the smaller dimension
    // used in some later broadcast experiments, this dataset is deliberately
    // large enough that the baseline focuses on shuffle-based joins.

    val customers =
      spark.range(1000000)
        .select(
          col("id").as("customer_id"),

          concat(
            lit("customer_"),
            col("id")
          ).as("customer_name"),

          // Generate three deterministic customer segments.
          when(
            col("id") % 3 === 0,
            lit("PREMIUM")
          )
            .when(
              col("id") % 3 === 1,
              lit("STANDARD")
            )
            .otherwise(
              lit("BASIC")
            )
            .as("segment")
        )

    // -----------------------------------------------------------------------
    // 5. Inspect dataset characteristics
    // -----------------------------------------------------------------------
    //
    // These counts are intentionally useful for validating the experiment.
    //
    // Note that count() is an action and therefore creates Spark jobs.
    // Similarly, rdd.getNumPartitions materializes the DataFrame's RDD
    // representation and should not be treated as a completely free
    // metadata lookup.
    //
    // These validation operations are separate from the timed join below.

    println()
    println("====================================================")
    println("DATASET INFORMATION")
    println("====================================================")

    println(
      s"Orders rows = ${orders.count()}"
    )

    println(
      s"Customers rows = ${customers.count()}"
    )

    println(
      s"Orders partitions = ${orders.rdd.getNumPartitions}"
    )

    println(
      s"Customers partitions = ${customers.rdd.getNumPartitions}"
    )

    // -----------------------------------------------------------------------
    // 6. Define the baseline join
    // -----------------------------------------------------------------------
    //
    // No join hint is provided.
    //
    // Because broadcast is explicitly disabled and AQE is disabled, Spark
    // should select a shuffle-based join strategy based on the available
    // logical/physical planning information.
    //
    // With these inputs, SortMergeJoin is the expected strategy.

    val joined =
      orders
        .join(
          customers,
          orders("customer_id") === customers("customer_id"),
          "inner"
        )
        .select(
          orders("order_id"),
          orders("customer_id"),
          orders("amount"),
          customers("customer_name"),
          customers("segment")
        )

    println()
    println("====================================================")
    println("LOGICAL JOIN")
    println("====================================================")

    println(
      "Orders INNER JOIN Customers ON customer_id"
    )

    // -----------------------------------------------------------------------
    // 7. Inspect the physical plan
    // -----------------------------------------------------------------------
    //
    // explain("formatted") is critical for this module.
    //
    // We expect to see:
    //
    //   SortMergeJoin
    //       |
    //       +-- Sort
    //       |     |
    //       |     +-- Exchange
    //       |
    //       +-- Sort
    //             |
    //             +-- Exchange
    //
    // Exchange represents the shuffle required to bring matching customer
    // IDs together.
    //
    // Sort prepares each shuffled side for the merge-based join.
    //
    // The plan, rather than the expected strategy printed below, is the
    // authoritative source for determining what Spark actually selected.

    println()
    println("====================================================")
    println("PHYSICAL PLAN")
    println("====================================================")

    joined.explain("formatted")

    // -----------------------------------------------------------------------
    // 8. Execute and measure the join
    // -----------------------------------------------------------------------
    //
    // Spark transformations are lazy. The join defined above has not
    // actually processed the data until an action is invoked.
    //
    // count() forces Spark to execute the complete join and count all
    // resulting rows.
    //
    // The timer intentionally starts immediately before the join action,
    // so the measurement focuses on this materialization rather than the
    // earlier dataset-validation actions.

    println()
    println("====================================================")
    println("EXECUTION")
    println("====================================================")

    val start = System.nanoTime()

    val resultCount =
      joined.count()

    val elapsedMs =
      (System.nanoTime() - start) / 1000000

    println(
      s"Joined rows = $resultCount"
    )

    println(
      s"Join execution time = ${elapsedMs} ms"
    )

    // -----------------------------------------------------------------------
    // 9. Final experiment summary
    // -----------------------------------------------------------------------
    //
    // This is a controlled baseline rather than a production performance
    // claim. The same workload and environment should be reused when
    // comparing later join optimizations.

    println()
    println("====================================================")
    println("FINAL RESULT")
    println("====================================================")

    println(
      "Expected join strategy: SortMergeJoin"
    )

    println(
      "Broadcast disabled and AQE disabled for baseline isolation."
    )

    println()
    println("Spark UI:")
    println("http://localhost:4040")

    println()
    println(
      "Keep the application running if Spark UI inspection " +
        "is required before stopping the application."
    )

    // -----------------------------------------------------------------------
    // 10. Clean shutdown
    // -----------------------------------------------------------------------

    spark.stop()
  }
}
