package com.shrikant.spark.joins

import com.shrikant.spark.foundation.EnterpriseSparkSession
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions._

/**
 * Module 1.6.4 — Sort Merge Join
 *
 * Demonstrates Spark's SortMergeJoin strategy using a large
 * fact-to-dimension join.
 *
 * Workload:
 *
 *   Fact / transaction dataset:
 *     - 10,000,000 rows
 *     - 100,000 customers
 *
 *   Customer dimension:
 *     - 100,000 rows
 *
 * Join:
 *
 *   fact.customer_id = customer.customer_id
 *
 * The broadcast threshold is configurable. A low threshold such as 1 MiB
 * can be used to make the customer relation ineligible for automatic
 * broadcast, allowing the shuffle-based SortMergeJoin strategy to be
 * observed.
 *
 * Expected physical plan:
 *
 *   SortMergeJoin
 *       |
 *       +---- Sort
 *       |      |
 *       |      +---- Exchange
 *       |             |
 *       |             +---- Fact
 *       |
 *       +---- Sort
 *              |
 *              +---- Exchange
 *                     |
 *                     +---- Customer
 *
 * Exchange:
 *   Redistributes both datasets by customer_id so that matching keys
 *   arrive at the same downstream partition.
 *
 * Sort:
 *   Sorts each shuffled side by the join key.
 *
 * SortMergeJoin:
 *   Merges the sorted streams and produces matching records.
 *
 * Engineering lesson:
 *
 * SortMergeJoin is a robust strategy for large-to-large joins because it
 * does not require the entire relation to fit into a broadcast or hash
 * structure. The trade-off is the cost of shuffle and sorting.
 *
 * Always validate the actual physical plan instead of assuming that the
 * configured threshold alone determines the final strategy.
 */
object SortMergeJoinExperiment {

  def main(args: Array[String]): Unit = {

    // -----------------------------------------------------------------------
    // 1. Parse experiment parameters
    // -----------------------------------------------------------------------

    // Automatic broadcast threshold in bytes.
    //
    // A value such as 1 MiB is useful for this experiment because it makes
    // automatic broadcasting less likely and allows the shuffle-based
    // join strategy to become visible.
    val broadcastThreshold: Long =
      if (args.nonEmpty) args(0).toLong
      else 1024L * 1024

    // Number of shuffle partitions.
    //
    // Keeping this configurable allows the experiment to be repeated with
    // different levels of shuffle parallelism.
    val shufflePartitions: Int =
      if (args.length > 1) args(1).toInt
      else 20

    // -----------------------------------------------------------------------
    // 2. Create Spark session
    // -----------------------------------------------------------------------

    val spark: SparkSession =
      EnterpriseSparkSession.create(
        "Sort Merge Join Experiment",
        "dev"
      )

    // -----------------------------------------------------------------------
    // 3. Configure Spark
    // -----------------------------------------------------------------------
    //
    // Configure the automatic broadcast threshold so that the experiment
    // can control whether the smaller customer relation is eligible for
    // automatic broadcasting.
    //
    // No join hint is used here.
    //
    // This is intentional: we want to observe Spark's physical planning
    // decision under the supplied configuration rather than explicitly
    // forcing a SortMergeJoin.

    spark.conf.set(
      "spark.sql.autoBroadcastJoinThreshold",
      broadcastThreshold.toString
    )

    // Keep shuffle parallelism explicit and reproducible.
    spark.conf.set(
      "spark.sql.shuffle.partitions",
      shufflePartitions.toString
    )

    // -----------------------------------------------------------------------
    // 4. Create the fact dataset
    // -----------------------------------------------------------------------
    //
    // Ten million deterministic transaction records.
    //
    // Every transaction is associated with one of 100,000 customers.

    val factDf: DataFrame =
      spark.range(0, 10000000)
        .select(
          col("id").alias("transaction_id"),

          // Join key.
          (col("id") % 100000).alias("customer_id"),

          // Deterministic transaction amount.
          (col("id") % 1000).alias("amount")
        )

    // -----------------------------------------------------------------------
    // 5. Create the customer dimension
    // -----------------------------------------------------------------------
    //
    // One hundred thousand customer records.
    //
    // Although this is considerably smaller than the fact dataset, the
    // configurable broadcast threshold allows us to study the behavior
    // when automatic broadcasting is not selected.

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
    // No explicit join hint is used.
    //
    // This allows Spark's optimizer to choose the physical strategy based
    // on the available configuration and statistics.
    //
    // When the customer side is not eligible for automatic broadcast,
    // SortMergeJoin is the expected strategy for this workload.

    val joinedDf =
      factDf.join(
        customerDf,
        Seq("customer_id"),
        "inner"
      )

    // -----------------------------------------------------------------------
    // 7. Print experiment configuration
    // -----------------------------------------------------------------------

    println()
    println("====================================================")
    println("MODULE 1.6.4 - SORT MERGE JOIN")
    println("====================================================")

    println()
    println("EXPERIMENT CONFIGURATION")
    println("----------------------------------------------------")

    println(
      s"spark.sql.autoBroadcastJoinThreshold = " +
        s"$broadcastThreshold bytes"
    )

    println(
      s"spark.sql.shuffle.partitions = $shufflePartitions"
    )

    // -----------------------------------------------------------------------
    // 8. Inspect the physical plan
    // -----------------------------------------------------------------------
    //
    // The physical plan is the primary evidence for this experiment.
    //
    // For SortMergeJoin, look for:
    //
    //   Exchange
    //       ↓
    //   Sort
    //       ↓
    //   SortMergeJoin
    //
    // on both sides of the join.
    //
    // Exchange represents the shuffle required to colocate matching
    // customer_id values.
    //
    // Sort represents ordering of each shuffled stream by the join key.
    //
    // SortMergeJoin then merges those sorted streams.
    //
    // If Spark chooses another strategy, the physical plan—not this
    // experiment's expectation—is the source of truth.

    println()
    println("====================================================")
    println("PHYSICAL PLAN")
    println("====================================================")

    joinedDf.explain("formatted")

    // -----------------------------------------------------------------------
    // 9. Execute the complete join
    // -----------------------------------------------------------------------
    //
    // Spark transformations are lazy.
    //
    // The join has not processed the data until the terminal action below.
    //
    // foreachPartition consumes the entire result, forcing Spark to execute
    // the complete join.

    println()
    println("====================================================")
    println("EXECUTION")
    println("====================================================")

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
    //
    // The elapsed time is useful for comparing controlled runs, but it is
    // not by itself sufficient to determine which join strategy is better.
    //
    // Spark UI metrics should also be inspected for:
    //
    //   - shuffle read/write
    //   - sort time
    //   - task duration
    //   - spill
    //   - partition distribution
    //   - executor memory
    //
    // Local Windows execution should not be interpreted as production
    // cluster performance.

    println()
    println("====================================================")
    println("EXECUTION SUMMARY")
    println("====================================================")

    println(
      s"Broadcast threshold = $broadcastThreshold bytes"
    )

    println(
      s"Shuffle partitions = $shufflePartitions"
    )

    println(
      "Rows joined = 10,000,000"
    )

    println(
      f"Execution time = $seconds%.3f s"
    )

    // -----------------------------------------------------------------------
    // 11. Spark UI
    // -----------------------------------------------------------------------
    //
    // Use the Spark UI to validate the physical costs of the join.
    //
    // For this experiment, pay particular attention to:
    //
    //   1. Exchange / shuffle volume
    //   2. Sort time
    //   3. Task duration distribution
    //   4. Spill memory and disk
    //   5. Number of runtime partitions
    //
    // These metrics explain the cost of SortMergeJoin much better than
    // elapsed time alone.

    println()
    println("====================================================")
    println("SPARK UI")
    println("====================================================")

    println("http://localhost:4040")

    println()
    println(
      "Inspect shuffle, sort, task and spill metrics in the Spark UI."
    )

    // -----------------------------------------------------------------------
    // 12. Clean shutdown
    // -----------------------------------------------------------------------

    spark.stop()
  }
}
