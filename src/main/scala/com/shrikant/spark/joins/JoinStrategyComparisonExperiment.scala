package com.shrikant.spark.joins

import com.shrikant.spark.foundation.EnterpriseSparkSession
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions._

/**
 * Module 1.6.8 — Join Strategy Comparison
 *
 * Compares three Spark join strategies using the same deterministic
 * workload:
 *
 *   1. Broadcast Hash Join
 *   2. Sort Merge Join
 *   3. Shuffled Hash Join
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
 * The strategy is explicitly selected through join hints so that the
 * experiment can compare physical execution strategies under controlled
 * conditions.
 *
 * This is intentionally NOT a cost-based ranking of the strategies.
 * Production performance depends on data size, cluster resources,
 * network characteristics, data distribution, memory pressure, skew,
 * serialization, and concurrent workloads.
 *
 * Expected physical plans:
 *
 *   BROADCAST
 *       BroadcastHashJoin
 *       BroadcastExchange
 *       No join-side shuffle of the fact relation
 *
 *   MERGE
 *       SortMergeJoin
 *       Exchange on both sides
 *       Sort on both sides
 *
 *   SHUFFLE_HASH
 *       ShuffledHashJoin
 *       Exchange on both sides
 *       No sort requirement
 *
 * Engineering lesson:
 *
 * Different join strategies solve the same logical problem using very
 * different physical execution mechanisms.
 *
 * The correct production workflow is:
 *
 *   Same workload
 *       ↓
 *   Force / select strategy
 *       ↓
 *   Inspect physical plan
 *       ↓
 *   Measure Spark UI metrics
 *       ↓
 *   Compare shuffle, sort, memory and task behavior
 *       ↓
 *   Make an engineering decision
 *
 * Do not select a join strategy based only on elapsed time from a small
 * local development environment.
 */
object JoinStrategyComparisonExperiment {

  def main(args: Array[String]): Unit = {

    // -----------------------------------------------------------------------
    // 1. Parse experiment parameters
    // -----------------------------------------------------------------------

    // Join strategy to compare.
    //
    // Supported values:
    //
    //   BROADCAST
    //   MERGE
    //   SHUFFLE_HASH
    //
    val strategy: String =
      if (args.nonEmpty) args(0).toUpperCase
      else "BROADCAST"

    // Automatic broadcast threshold in bytes.
    //
    // This is kept configurable so that the same experiment can be
    // reproduced under different Spark planning configurations.
    val broadcastThreshold: Long =
      if (args.length > 1) args(1).toLong
      else 10L * 1024 * 1024

    // Number of shuffle partitions used by shuffle-based strategies.
    val shufflePartitions: Int =
      if (args.length > 2) args(2).toInt
      else 20

    // -----------------------------------------------------------------------
    // 2. Validate strategy
    // -----------------------------------------------------------------------

    val supportedStrategies =
      Set(
        "BROADCAST",
        "MERGE",
        "SHUFFLE_HASH"
      )

    if (!supportedStrategies.contains(strategy)) {
      throw new IllegalArgumentException(
        s"Unknown strategy: $strategy. " +
          s"Supported strategies: ${supportedStrategies.mkString(", ")}"
      )
    }

    // -----------------------------------------------------------------------
    // 3. Create Spark session
    // -----------------------------------------------------------------------

    val spark: SparkSession =
      EnterpriseSparkSession.create(
        "Join Strategy Comparison",
        "dev"
      )

    // -----------------------------------------------------------------------
    // 4. Configure controlled execution conditions
    // -----------------------------------------------------------------------
    //
    // The same shuffle partition count and broadcast threshold are used
    // across the comparison runs.
    //
    // This helps isolate the physical join strategy as the main variable.
    //
    // Note:
    //
    // The BROADCAST hint can explicitly request broadcasting even when the
    // automatic broadcast threshold would normally prevent the optimizer
    // from selecting it.

    spark.conf.set(
      "spark.sql.autoBroadcastJoinThreshold",
      broadcastThreshold.toString
    )

    spark.conf.set(
      "spark.sql.shuffle.partitions",
      shufflePartitions.toString
    )

    // -----------------------------------------------------------------------
    // 5. Create the fact dataset
    // -----------------------------------------------------------------------
    //
    // Ten million deterministic transaction records.
    //
    // Each transaction is mapped to one of 100,000 customers.

    val factDf: DataFrame =
      spark.range(0, 10000000)
        .select(
          col("id").alias("transaction_id"),

          // Map transactions to customer IDs.
          (col("id") % 100000).alias("customer_id"),

          // Deterministic transaction amount.
          (col("id") % 1000).alias("amount")
        )

    // -----------------------------------------------------------------------
    // 6. Create the customer dimension
    // -----------------------------------------------------------------------
    //
    // One hundred thousand customer records.
    //
    // This is significantly smaller than the fact dataset and is therefore
    // the natural candidate for the broadcast or hash build side.

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
    // 7. Apply the requested physical join strategy
    // -----------------------------------------------------------------------
    //
    // Each strategy is deliberately forced through a join hint.
    //
    // BROADCAST:
    //   Broadcast the smaller customer dimension.
    //
    // MERGE:
    //   Request SortMergeJoin on both relations.
    //
    // SHUFFLE_HASH:
    //   Request ShuffledHashJoin on both relations.
    //
    // This makes the experiment a controlled strategy comparison rather
    // than a test of Spark's automatic strategy selection.

    val joinedDf: DataFrame =
      strategy match {

        case "BROADCAST" =>
          factDf.join(
            customerDf.hint("BROADCAST"),
            Seq("customer_id"),
            "inner"
          )

        case "MERGE" =>
          factDf
            .hint("MERGE")
            .join(
              customerDf.hint("MERGE"),
              Seq("customer_id"),
              "inner"
            )

        case "SHUFFLE_HASH" =>
          factDf
            .hint("SHUFFLE_HASH")
            .join(
              customerDf.hint("SHUFFLE_HASH"),
              Seq("customer_id"),
              "inner"
            )
      }

    // -----------------------------------------------------------------------
    // 8. Print experiment configuration
    // -----------------------------------------------------------------------

    println()
    println("====================================================")
    println(
      s"MODULE 1.6.8 - JOIN STRATEGY COMPARISON | " +
        s"strategy=$strategy"
    )
    println("====================================================")

    println()
    println("EXPERIMENT CONFIGURATION")
    println("----------------------------------------------------")
    println(s"Strategy = $strategy")
    println(
      s"spark.sql.autoBroadcastJoinThreshold = " +
        s"$broadcastThreshold bytes"
    )
    println(
      s"spark.sql.shuffle.partitions = $shufflePartitions"
    )

    // -----------------------------------------------------------------------
    // 9. Inspect the physical plan
    // -----------------------------------------------------------------------
    //
    // The physical plan is the first piece of evidence.
    //
    // For BROADCAST, look for:
    //
    //   BroadcastHashJoin
    //   BroadcastExchange
    //
    // For MERGE, look for:
    //
    //   SortMergeJoin
    //   Exchange
    //   Sort
    //
    // For SHUFFLE_HASH, look for:
    //
    //   ShuffledHashJoin
    //   Exchange
    //
    // The plan tells us HOW Spark intends to execute the join.
    // The Spark UI tells us HOW that execution actually behaved.

    println()
    println("====================================================")
    println("PHYSICAL PLAN")
    println("====================================================")

    joinedDf.explain("formatted")

    // -----------------------------------------------------------------------
    // 10. Execute the complete join
    // -----------------------------------------------------------------------
    //
    // Spark transformations are lazy.
    //
    // The join above has not processed the data yet.
    //
    // foreachPartition is a terminal action that consumes every result row,
    // forcing the complete join to execute.

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
    // 11. Print execution summary
    // -----------------------------------------------------------------------
    //
    // The elapsed time is useful as one measurement, but it should not be
    // interpreted in isolation.
    //
    // For the comparison, inspect the Spark UI for:
    //
    //   - shuffle read/write
    //   - task duration
    //   - task distribution
    //   - sort time
    //   - spill
    //   - executor memory usage
    //   - broadcast size
    //   - number of output partitions
    //
    // These metrics provide much stronger evidence for why one strategy
    // behaved differently from another.

    println()
    println("====================================================")
    println("EXECUTION SUMMARY")
    println("====================================================")

    println(s"Strategy = $strategy")
    println(s"Rows joined = 10,000,000")
    println(f"Execution time = $seconds%.3f s")

    // -----------------------------------------------------------------------
    // 12. Spark UI
    // -----------------------------------------------------------------------
    //
    // Keep the UI available when investigating stage-level behavior.
    //
    // The local UI is particularly useful for visually comparing:
    //
    //   BROADCAST
    //       -> broadcast exchange
    //
    //   MERGE
    //       -> shuffle + sort on both sides
    //
    //   SHUFFLE_HASH
    //       -> shuffle on both sides + hash build
    //
    // Remember that local driver/executor measurements are not representative
    // of a distributed production cluster.

    println()
    println("Spark UI:")
    println("http://localhost:4040")

    println()
    println(
      "Inspect Spark UI stage/task metrics before comparing strategies."
    )

    // -----------------------------------------------------------------------
    // 13. Stop Spark
    // -----------------------------------------------------------------------

    spark.stop()
  }
}
