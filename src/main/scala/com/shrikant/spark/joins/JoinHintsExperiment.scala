package com.shrikant.spark.joins

import com.shrikant.spark.foundation.EnterpriseSparkSession
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions._

/**
 * Module 1.6.6 — Join Hints
 *
 * Demonstrates how Spark SQL join hints can influence the physical join
 * strategy selected by the optimizer.
 *
 * Supported hints:
 *
 *   BROADCAST
 *       Forces/strongly encourages the hinted relation to be broadcast,
 *       allowing Spark to use BroadcastHashJoin when the join is eligible.
 *
 *   MERGE
 *       Requests a SortMergeJoin.
 *
 *   SHUFFLE_HASH
 *       Requests a ShuffledHashJoin.
 *
 * Workload:
 *
 *   Fact / Transaction dataset:
 *     - 10,000,000 rows
 *     - 100,000 customers
 *
 *   Customer dimension:
 *     - 100,000 rows
 *
 * The experiment intentionally uses a configurable broadcast threshold
 * so that BROADCAST can be compared against MERGE and SHUFFLE_HASH even
 * when the normal automatic broadcast threshold would otherwise prevent
 * broadcasting.
 *
 * Expected strategies:
 *
 *   BROADCAST
 *       -> BroadcastHashJoin
 *
 *   MERGE
 *       -> SortMergeJoin
 *
 *   SHUFFLE_HASH
 *       -> ShuffledHashJoin
 *
 * Engineering lesson:
 *
 * Join hints are optimization overrides, not a substitute for investigation.
 *
 * A production workflow should be:
 *
 *   Hint
 *      ↓
 *   Inspect physical plan
 *      ↓
 *   Measure Spark UI runtime behavior
 *      ↓
 *   Validate against realistic data volumes
 *      ↓
 *   Monitor after deployment
 *
 * A hint that improves today's workload can become harmful when the
 * underlying data distribution or dimension size changes.
 */
object JoinHintsExperiment {

  def main(args: Array[String]): Unit = {

    // -----------------------------------------------------------------------
    // 1. Parse experiment parameters
    // -----------------------------------------------------------------------

    // Join hint to apply.
    //
    // Supported values:
    //
    //   BROADCAST
    //   MERGE
    //   SHUFFLE_HASH
    //
    val joinHint: String =
      if (args.nonEmpty) args(0).toUpperCase
      else "BROADCAST"

    // Broadcast threshold in bytes.
    //
    // This is particularly useful when comparing the BROADCAST hint with
    // the other strategies under controlled conditions.
    val broadcastThreshold: Long =
      if (args.length > 1) args(1).toLong
      else 1024L * 1024

    // Number of shuffle partitions used by shuffle-based strategies.
    val shufflePartitions: Int =
      if (args.length > 2) args(2).toInt
      else 20

    // -----------------------------------------------------------------------
    // 2. Create Spark session
    // -----------------------------------------------------------------------

    val spark: SparkSession =
      EnterpriseSparkSession.create(
        "Join Hints Experiment",
        "dev"
      )

    // -----------------------------------------------------------------------
    // 3. Configure Spark
    // -----------------------------------------------------------------------

    // Keep the broadcast threshold explicit so that each experiment can
    // reproduce the same planning conditions.
    //
    // Note that a BROADCAST hint can override the normal automatic
    // broadcast threshold for an eligible relation. The threshold is
    // therefore not equivalent to "broadcast is allowed only below this
    // size" when a BROADCAST hint is present.
    spark.conf.set(
      "spark.sql.autoBroadcastJoinThreshold",
      broadcastThreshold.toString
    )

    // Keep shuffle parallelism constant across the experiments.
    spark.conf.set(
      "spark.sql.shuffle.partitions",
      shufflePartitions.toString
    )

    // -----------------------------------------------------------------------
    // 4. Create the large fact dataset
    // -----------------------------------------------------------------------
    //
    // This represents a transaction/fact table.
    //
    // 10 million rows are mapped to 100,000 customers.

    val factDf: DataFrame =
      spark.range(0, 10000000)
        .select(
          col("id").alias("transaction_id"),

          // Deterministically map transactions to customers.
          (col("id") % 100000).alias("customer_id"),

          // Deterministic transaction amount.
          (col("id") % 1000).alias("amount")
        )

    // -----------------------------------------------------------------------
    // 5. Create the customer dimension
    // -----------------------------------------------------------------------
    //
    // The customer relation contains 100,000 rows and is intentionally
    // much smaller than the 10-million-row fact relation.
    //
    // This makes it a natural candidate for broadcast or hash build-side
    // selection.

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
    // 6. Apply the requested join hint
    // -----------------------------------------------------------------------
    //
    // The hint is applied to the appropriate relation.
    //
    // BROADCAST:
    //   Broadcast only the smaller customer dimension.
    //
    // MERGE:
    //   Apply MERGE to both sides so that Spark is explicitly asked to use
    //   a merge-based join.
    //
    // SHUFFLE_HASH:
    //   Apply SHUFFLE_HASH to both sides so that Spark is explicitly asked
    //   to use a shuffle-hash join.
    //
    // No hint is applied to the fact side for BROADCAST because broadcasting
    // the 10-million-row fact relation would be an inappropriate production
    // decision for this workload.

    val hintedFact: DataFrame =
      joinHint match {
        case "MERGE" =>
          factDf.hint("MERGE")

        case "SHUFFLE_HASH" =>
          factDf.hint("SHUFFLE_HASH")

        case _ =>
          factDf
      }

    val hintedCustomer: DataFrame =
      joinHint match {
        case "BROADCAST" =>
          customerDf.hint("BROADCAST")

        case "MERGE" =>
          customerDf.hint("MERGE")

        case "SHUFFLE_HASH" =>
          customerDf.hint("SHUFFLE_HASH")

        case _ =>
          customerDf
      }

    // -----------------------------------------------------------------------
    // 7. Define the join
    // -----------------------------------------------------------------------
    //
    // No count/show/action has been executed yet.
    //
    // Spark transformations are lazy, so the physical execution will occur
    // only when the foreachPartition action below is triggered.

    val joinedDf =
      hintedFact.join(
        hintedCustomer,
        Seq("customer_id"),
        "inner"
      )

    println()
    println(
      s"=== Module 1.6.6 — Join Hints | " +
        s"hint=$joinHint | " +
        s"threshold=$broadcastThreshold | " +
        s"shufflePartitions=$shufflePartitions ==="
    )

    // -----------------------------------------------------------------------
    // 8. Inspect the physical plan
    // -----------------------------------------------------------------------
    //
    // This is the primary evidence for determining whether the requested
    // hint influenced the physical execution strategy.
    //
    // Expected operators:
    //
    //   BROADCAST
    //       BroadcastHashJoin
    //       BroadcastExchange
    //
    //   MERGE
    //       SortMergeJoin
    //       Exchange
    //       Sort
    //
    //   SHUFFLE_HASH
    //       ShuffledHashJoin
    //       Exchange
    //
    // The actual physical plan should always be treated as authoritative.

    println()
    println("Physical plan:")
    joinedDf.explain("formatted")

    // -----------------------------------------------------------------------
    // 9. Execute the join
    // -----------------------------------------------------------------------
    //
    // foreachPartition is a terminal action.
    //
    // The iterator is consumed completely so that Spark must execute the
    // entire join rather than stopping after a subset of the result.

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
    println("=== Execution Summary ===")
    println(s"Join hint = $joinHint")
    println(s"Broadcast threshold = $broadcastThreshold bytes")
    println(s"Shuffle partitions = $shufflePartitions")
    println("Rows joined = 10,000,000")
    println(f"Execution time = $seconds%.3f s")

    // -----------------------------------------------------------------------
    // 11. Engineering reminder
    // -----------------------------------------------------------------------
    //
    // The execution time from this local experiment should not be treated
    // as a universal ranking of join strategies.
    //
    // Production performance depends on:
    //
    //   - dataset size
    //   - data distribution
    //   - cluster size
    //   - executor memory
    //   - network bandwidth
    //   - shuffle volume
    //   - serialization
    //   - skew
    //   - concurrent workloads
    //
    // Spark UI task/stage metrics should therefore be inspected together
    // with the physical plan.

    println()
    println("Spark UI:")
    println("http://localhost:4040")

    // -----------------------------------------------------------------------
    // 12. Stop Spark
    // -----------------------------------------------------------------------

    spark.stop()
  }
}
