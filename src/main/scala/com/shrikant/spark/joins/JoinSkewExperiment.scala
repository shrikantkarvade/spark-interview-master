package com.shrikant.spark.joins

import com.shrikant.spark.foundation.EnterpriseSparkSession
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions._

/**
 * Module 1.6.9 — Join Skew
 *
 * Demonstrates how an uneven distribution of join keys can create
 * partition-level workload imbalance during a shuffle-based join.
 *
 * Workload:
 *
 *   Fact / transaction dataset:
 *     - 10,000,000 rows
 *     - Highly skewed customer_id distribution
 *
 *   Customer dimension:
 *     - 100,000 rows
 *     - Relatively uniform distribution
 *
 * Skew distribution on the fact side:
 *
 *   customer_id = 0 -> 5,000,000 rows (50%)
 *   customer_id = 1 -> 1,000,000 rows (10%)
 *   customer_id = 2 ->   500,000 rows (5%)
 *   remaining keys  -> 3,500,000 rows (35%)
 *
 * This deliberately creates a few "hot keys" that receive a much larger
 * amount of data than the normal keys.
 *
 * Join strategy:
 *
 *   SortMergeJoin
 *
 * AQE configuration:
 *
 *   spark.sql.adaptive.enabled = true
 *   spark.sql.adaptive.skewJoin.enabled = false
 *
 * AQE itself remains enabled because later experiments demonstrate how
 * Adaptive Query Execution can mitigate skew. However, skew-join handling
 * is explicitly disabled here so that this experiment establishes the
 * unmitigated skew baseline.
 *
 * Expected physical plan:
 *
 *   SortMergeJoin
 *       |
 *       +---- Exchange + Sort (fact)
 *       |
 *       +---- Exchange + Sort (customer)
 *
 * Expected runtime behavior:
 *
 *   Some shuffle partitions contain significantly more rows than others.
 *   This can result in a large difference between the fastest and slowest
 *   tasks in the join stage.
 *
 * Engineering lesson:
 *
 * Increasing spark.sql.shuffle.partitions changes the number of shuffle
 * partitions, but it does not automatically split records belonging to
 * one hot join key across multiple partitions.
 *
 * Therefore:
 *
 *   More partitions != automatic skew resolution
 *
 * The Spark UI task distribution is essential evidence for diagnosing
 * this problem.
 */
object JoinSkewExperiment {

  def main(args: Array[String]): Unit = {

    // -----------------------------------------------------------------------
    // 1. Parse experiment parameters
    // -----------------------------------------------------------------------

    // Number of shuffle partitions.
    //
    // This is configurable so the experiment can be repeated with different
    // partition counts while keeping the data distribution unchanged.
    val shufflePartitions: Int =
      if (args.nonEmpty) args(0).toInt
      else 20

    // -----------------------------------------------------------------------
    // 2. Create Spark session
    // -----------------------------------------------------------------------

    val spark: SparkSession =
      EnterpriseSparkSession.create(
        "Join Skew Experiment",
        "dev"
      )

    // -----------------------------------------------------------------------
    // 3. Configure a controlled skew baseline
    // -----------------------------------------------------------------------
    //
    // Broadcasting is disabled because the experiment is specifically about
    // shuffle-induced skew.
    //
    // AQE remains enabled, but its skew-join optimization is disabled.
    // This allows us to observe the skew before applying the mitigation
    // demonstrated in Module 1.6.10.

    spark.conf.set(
      "spark.sql.shuffle.partitions",
      shufflePartitions.toString
    )

    spark.conf.set(
      "spark.sql.autoBroadcastJoinThreshold",
      "-1"
    )

    spark.conf.set(
      "spark.sql.adaptive.enabled",
      "true"
    )

    spark.conf.set(
      "spark.sql.adaptive.skewJoin.enabled",
      "false"
    )

    // -----------------------------------------------------------------------
    // 4. Create the intentionally skewed fact dataset
    // -----------------------------------------------------------------------
    //
    // The first 6.5 million records deliberately concentrate on three
    // customer IDs:
    //
    //   customer 0 -> 5.0M rows
    //   customer 1 -> 1.0M rows
    //   customer 2 -> 0.5M rows
    //
    // The remaining 3.5M rows are distributed across many other keys.
    //
    // When Spark performs the shuffle on customer_id, all records belonging
    // to the same key are mapped to the same logical shuffle partition.
    //
    // This is the core reason a single hot key can create a large partition
    // even when the overall average partition size appears reasonable.

    val factDf: DataFrame =
      spark.range(0, 10000000)
        .select(
          col("id").alias("transaction_id"),

          when(
            col("id") < 5000000,
            lit(0L)
          )
            .when(
              col("id") < 6000000,
              lit(1L)
            )
            .when(
              col("id") < 6500000,
              lit(2L)
            )
            .otherwise(
              (col("id") % 99997) + 3
            )
            .alias("customer_id"),

          (col("id") % 1000).alias("amount")
        )

    // -----------------------------------------------------------------------
    // 5. Create the customer dimension
    // -----------------------------------------------------------------------
    //
    // The dimension contains 100,000 customer records.
    //
    // The dimension itself is relatively uniform. The skew is deliberately
    // introduced on the fact side so that the effect of the hot join keys
    // can be isolated.

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
    // 6. Force a shuffle-based join
    // -----------------------------------------------------------------------
    //
    // MERGE hints are applied to both relations to keep the experiment
    // focused on SortMergeJoin.
    //
    // Broadcasting is already disabled, but the explicit MERGE hints make
    // the experiment's intent clear and provide a controlled comparison
    // point for Module 1.6.10.

    val joinedDf =
      factDf
        .hint("MERGE")
        .join(
          customerDf.hint("MERGE"),
          Seq("customer_id"),
          "inner"
        )

    println()
    println(
      s"=== Module 1.6.9 — Join Skew | " +
        s"partitions=$shufflePartitions ==="
    )

    // -----------------------------------------------------------------------
    // 7. Inspect the physical plan
    // -----------------------------------------------------------------------
    //
    // The expected strategy is SortMergeJoin.
    //
    // Look for:
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
    // At this stage the plan alone does not prove that skew exists.
    //
    // The skew is a runtime data-distribution problem, so the Spark UI
    // task-level metrics are required to demonstrate its impact.

    println()
    println("Physical plan:")
    joinedDf.explain("formatted")

    // -----------------------------------------------------------------------
    // 8. Execute the complete join
    // -----------------------------------------------------------------------
    //
    // foreachPartition is used as the terminal action.
    //
    // Every result row is consumed, forcing Spark to execute the complete
    // join workload.

    val start = System.nanoTime()

    joinedDf.foreachPartition { rows: Iterator[Row] =>
      while (rows.hasNext) {
        rows.next()
      }
    }

    val seconds =
      (System.nanoTime() - start) / 1e9

    // -----------------------------------------------------------------------
    // 9. Execution summary
    // -----------------------------------------------------------------------

    println()
    println("=== Execution Summary ===")
    println(s"Shuffle partitions = $shufflePartitions")
    println("Rows joined = 10,000,000")
    println(f"Execution time = $seconds%.3f s")

    // -----------------------------------------------------------------------
    // 10. Runtime investigation guidance
    // -----------------------------------------------------------------------
    //
    // The most important evidence for this experiment is in the Spark UI.
    //
    // Inspect the join stage and compare:
    //
    //   - task duration
    //   - task input/shuffle read
    //   - partition-level record counts
    //   - minimum task duration
    //   - median task duration
    //   - maximum task duration
    //
    // A significant max/median or max/average difference indicates that
    // some tasks are processing substantially more data than others.
    //
    // This is the runtime manifestation of data skew.
    //
    // Important:
    //
    // Increasing shuffle partitions does not guarantee that customer_id=0
    // will be divided across multiple reducers. Records for the same join
    // key must remain together for a normal equality join.
    //
    // Module 1.6.10 introduces AQE skew-join handling, which can split
    // oversized shuffle partitions after runtime statistics are available.

    println()
    println("Spark UI:")
    println("http://localhost:4040")

    println()
    println(
      "Inspect task min/median/max duration and shuffle-read " +
        "distribution in the Spark UI."
    )

    // -----------------------------------------------------------------------
    // 11. Stop Spark
    // -----------------------------------------------------------------------

    spark.stop()
  }
}
