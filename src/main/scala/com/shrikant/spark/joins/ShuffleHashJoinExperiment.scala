package com.shrikant.spark.joins

import com.shrikant.spark.foundation.EnterpriseSparkSession
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions._

/**
 * Module 1.6.5 — Shuffle Hash Join
 *
 * Demonstrates Spark's ShuffledHashJoin strategy for a large
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
 * Configuration:
 *
 *   spark.sql.autoBroadcastJoinThreshold
 *       Controls automatic broadcast eligibility.
 *
 *   spark.sql.shuffle.partitions
 *       Controls the number of shuffle partitions.
 *
 *   spark.sql.join.preferSortMergeJoin
 *       Controls Spark's preference between SortMergeJoin and
 *       ShuffledHashJoin when both strategies are eligible.
 *
 * The default experiment sets preferSortMergeJoin=false so that Spark
 * has a preference toward ShuffledHashJoin.
 *
 * Expected physical plan:
 *
 *   ShuffledHashJoin
 *       |
 *       +---- Exchange (fact)
 *       |
 *       +---- Exchange (customer)
 *
 * Unlike SortMergeJoin, ShuffledHashJoin does not require sorting both
 * shuffled relations. Instead, Spark builds an in-memory hash structure
 * for the selected build side.
 *
 * Important:
 *
 * spark.sql.join.preferSortMergeJoin=false is a preference, not a
 * guarantee that Spark will always select ShuffledHashJoin.
 *
 * Join eligibility, statistics, build-side requirements, and other
 * optimizer rules still influence the final physical plan.
 *
 * Therefore, the physical plan is the authoritative evidence.
 *
 * Engineering lesson:
 *
 * Shuffle Hash Join can avoid the sorting cost of SortMergeJoin, but
 * the build-side hash structure requires executor memory. The strategy
 * should therefore be evaluated using both shuffle metrics and memory/
 * spill behavior.
 */
object ShuffleHashJoinExperiment {

  def main(args: Array[String]): Unit = {

    // -----------------------------------------------------------------------
    // 1. Parse experiment parameters
    // -----------------------------------------------------------------------

    // Automatic broadcast threshold in bytes.
    //
    // A low threshold can be used to prevent the optimizer from naturally
    // selecting a BroadcastHashJoin for this experiment.
    val broadcastThreshold: Long =
      if (args.nonEmpty) args(0).toLong
      else 1024L * 1024

    // Number of shuffle partitions.
    val shufflePartitions: Int =
      if (args.length > 1) args(1).toInt
      else 20

    // Preference between SortMergeJoin and ShuffledHashJoin.
    //
    // false:
    //   Prefer ShuffledHashJoin when both strategies are eligible.
    //
    // true:
    //   Prefer SortMergeJoin.
    //
    // This setting is a preference, not a hard physical-plan override.
    val preferSortMergeJoin: Boolean =
      if (args.length > 2) args(2).toBoolean
      else false

    // -----------------------------------------------------------------------
    // 2. Create Spark session
    // -----------------------------------------------------------------------

    val spark: SparkSession =
      EnterpriseSparkSession.create(
        "Shuffle Hash Join Experiment",
        "dev"
      )

    // -----------------------------------------------------------------------
    // 3. Configure join behavior
    // -----------------------------------------------------------------------

    // Keep the automatic broadcast threshold explicit.
    //
    // The experiment can therefore be repeated with different thresholds
    // to understand how broadcast eligibility interacts with join strategy.
    spark.conf.set(
      "spark.sql.autoBroadcastJoinThreshold",
      broadcastThreshold.toString
    )

    // Keep shuffle parallelism controlled across runs.
    spark.conf.set(
      "spark.sql.shuffle.partitions",
      shufflePartitions.toString
    )

    // Tell Spark which shuffle-based strategy it should prefer when the
    // optimizer considers SortMergeJoin and ShuffledHashJoin.
    //
    // This does NOT guarantee a ShuffledHashJoin.
    spark.conf.set(
      "spark.sql.join.preferSortMergeJoin",
      preferSortMergeJoin.toString
    )

    // -----------------------------------------------------------------------
    // 4. Create the fact dataset
    // -----------------------------------------------------------------------
    //
    // Ten million transaction records are mapped to 100,000 customers.
    //
    // This represents the large side of a typical fact-to-dimension join.

    val factDf: DataFrame =
      spark.range(0, 10000000)
        .select(
          col("id").alias("transaction_id"),

          // Map each transaction to one of 100,000 customers.
          (col("id") % 100000).alias("customer_id"),

          // Deterministic transaction amount.
          (col("id") % 1000).alias("amount")
        )

    // -----------------------------------------------------------------------
    // 5. Create the customer dimension
    // -----------------------------------------------------------------------
    //
    // The smaller customer relation is a natural candidate for the hash
    // build side.
    //
    // In a ShuffledHashJoin, the selected build relation is shuffled and
    // then materialized into hash structures within the relevant tasks.

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
    // No join hint is used here.
    //
    // This is intentional.
    //
    // Unlike Module 1.6.6 and Module 1.6.8, this experiment demonstrates
    // how Spark's configuration preference can influence strategy selection
    // without explicitly forcing the join through a hint.

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
    println("MODULE 1.6.5 - SHUFFLE HASH JOIN")
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

    println(
      s"spark.sql.join.preferSortMergeJoin = " +
        s"$preferSortMergeJoin"
    )

    // -----------------------------------------------------------------------
    // 8. Inspect the physical plan
    // -----------------------------------------------------------------------
    //
    // The expected plan when the configuration and data make SHJ eligible is:
    //
    //   ShuffledHashJoin
    //       |
    //       +-- Exchange
    //       |
    //       +-- Exchange
    //
    // Compare this with SortMergeJoin:
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
    // The important difference is that ShuffledHashJoin avoids the explicit
    // sorting phase but must build a hash structure for the build relation.
    //
    // Always validate the actual operator in the physical plan.

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
    // The join is executed only when this terminal action is invoked.
    //
    // foreachPartition consumes every result row, ensuring the complete
    // join workload is executed.

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
    // 10. Execution summary
    // -----------------------------------------------------------------------

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
      s"Prefer SortMergeJoin = $preferSortMergeJoin"
    )

    println(
      "Rows joined = 10,000,000"
    )

    println(
      f"Execution time = $seconds%.3f s"
    )

    // -----------------------------------------------------------------------
    // 11. Spark UI investigation
    // -----------------------------------------------------------------------
    //
    // Physical-plan inspection tells us that a ShuffledHashJoin was selected.
    //
    // The Spark UI should then be used to investigate WHY the strategy
    // performed the way it did.
    //
    // Important metrics:
    //
    //   - Shuffle read
    //   - Shuffle write
    //   - Task duration distribution
    //   - Hash-build time
    //   - Peak execution memory
    //   - Spill memory
    //   - Spill disk
    //
    // In particular, monitor memory usage because the build-side hash
    // structure can require substantially more memory than the serialized
    // shuffle data itself.

    println()
    println("====================================================")
    println("SPARK UI")
    println("====================================================")

    println("http://localhost:4040")

    println()
    println(
      "Inspect shuffle, hash-build, memory and spill metrics " +
        "in the Spark UI."
    )

    // -----------------------------------------------------------------------
    // 12. Clean shutdown
    // -----------------------------------------------------------------------

    spark.stop()
  }
}
