package com.shrikant.spark.joins

import com.shrikant.spark.foundation.EnterpriseSparkSession
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions._

/**
 * Module 1.6.12 — Production Join Optimization
 *
 * Capstone experiment for Module 1.6.
 *
 * This experiment brings together the join optimization techniques explored
 * throughout the module and evaluates them as production-style scenarios.
 *
 * Scenarios:
 *
 *   NATURAL
 *       Static broadcast is disabled.
 *       AQE is enabled and may convert the initial SortMergeJoin into a
 *       BroadcastHashJoin after runtime statistics become available.
 *
 *   BROADCAST
 *       Explicitly broadcasts the smaller customer dimension.
 *
 *   MERGE
 *       Explicitly requests SortMergeJoin.
 *
 *   SHUFFLE_HASH
 *       Explicitly requests ShuffledHashJoin.
 *
 *   SKEW
 *       Uses a highly skewed fact dataset and enables AQE skew-join
 *       optimization.
 *
 * Common workload:
 *
 *   Normal scenarios:
 *     - 10,000,000 fact rows
 *     - 100,000 customer rows
 *
 *   Skew scenario:
 *     - 10,000,000 fact rows
 *     - 90% of fact rows use customer_id = 0
 *     - Remaining rows are distributed across many customer IDs
 *
 * The purpose is not to identify one universally "best" join strategy.
 * Instead, the experiment demonstrates that join optimization is a
 * scenario-dependent engineering decision.
 *
 * Production decision framework:
 *
 *   1. Understand data size and distribution
 *   2. Inspect the initial physical plan
 *   3. Understand whether Spark can broadcast
 *   4. Check for shuffle and sort costs
 *   5. Investigate build-side memory requirements
 *   6. Look for data skew
 *   7. Enable/validate AQE where appropriate
 *   8. Measure the actual Spark UI behavior
 *   9. Validate the decision against realistic production data
 *
 * Core engineering principle:
 *
 *   Do not optimize the logical SQL in isolation.
 *   Optimize and validate the physical execution.
 */
object ProductionJoinOptimizationExperiment {

  /**
   * Defines the configuration for one production-style join scenario.
   *
   * @param name
   *   Scenario name.
   *
   * @param description
   *   Human-readable explanation of the scenario.
   *
   * @param staticBroadcastThreshold
   *   Value for spark.sql.autoBroadcastJoinThreshold.
   *
   * @param adaptiveBroadcastThreshold
   *   Value for spark.sql.adaptive.autoBroadcastJoinThreshold.
   *
   * @param hint
   *   Optional join hint used to explicitly influence the physical strategy.
   *
   * @param skewJoinEnabled
   *   Whether AQE skew-join optimization is enabled.
   */
  case class Scenario(
                       name: String,
                       description: String,
                       staticBroadcastThreshold: Long,
                       adaptiveBroadcastThreshold: Long,
                       hint: Option[String],
                       skewJoinEnabled: Boolean
                     )

  def main(args: Array[String]): Unit = {

    // -----------------------------------------------------------------------
    // 1. Parse command-line arguments
    // -----------------------------------------------------------------------

    // Scenario to execute.
    //
    // Supported values:
    //
    //   NATURAL
    //   BROADCAST
    //   MERGE
    //   SHUFFLE_HASH
    //   SKEW
    //
    val scenarioName: String =
      if (args.nonEmpty) args(0).toUpperCase
      else "NATURAL"

    // Number of shuffle partitions.
    val shufflePartitions: Int =
      if (args.length > 1) args(1).toInt
      else 20

    // Optional pause after execution.
    //
    // This is useful when Spark UI inspection is required before the
    // application shuts down.
    val uiPauseSeconds: Int =
      args
        .drop(2)
        .find(_.startsWith("--ui-pause="))
        .map(_.stripPrefix("--ui-pause=").toInt)
        .getOrElse(0)

    // -----------------------------------------------------------------------
    // 2. Define production-style scenarios
    // -----------------------------------------------------------------------
    //
    // Each scenario represents a different engineering decision.
    //
    // NATURAL:
    //   Let AQE use runtime statistics to potentially improve the initial
    //   plan.
    //
    // BROADCAST:
    //   Explicitly choose the small customer dimension as the broadcast side.
    //
    // MERGE:
    //   Force a shuffle-based SortMergeJoin.
    //
    // SHUFFLE_HASH:
    //   Force a shuffle-based hash join.
    //
    // SKEW:
    //   Demonstrate AQE's ability to mitigate an oversized/skewed shuffle
    //   partition.

    val scenarios: Map[String, Scenario] =
      Map(
        "NATURAL" ->
          Scenario(
            name = "NATURAL",
            description =
              "AQE may convert the initial SMJ to BHJ using runtime statistics.",
            staticBroadcastThreshold = -1,
            adaptiveBroadcastThreshold = 10L * 1024 * 1024,
            hint = None,
            skewJoinEnabled = false
          ),

        "BROADCAST" ->
          Scenario(
            name = "BROADCAST",
            description =
              "Explicitly broadcast the smaller customer dimension.",
            staticBroadcastThreshold = 1024L * 1024,
            adaptiveBroadcastThreshold = -1,
            hint = Some("BROADCAST"),
            skewJoinEnabled = false
          ),

        "MERGE" ->
          Scenario(
            name = "MERGE",
            description =
              "Explicitly request SortMergeJoin.",
            staticBroadcastThreshold = 1024L * 1024,
            adaptiveBroadcastThreshold = -1,
            hint = Some("MERGE"),
            skewJoinEnabled = false
          ),

        "SHUFFLE_HASH" ->
          Scenario(
            name = "SHUFFLE_HASH",
            description =
              "Explicitly request ShuffledHashJoin.",
            staticBroadcastThreshold = 1024L * 1024,
            adaptiveBroadcastThreshold = -1,
            hint = Some("SHUFFLE_HASH"),
            skewJoinEnabled = false
          ),

        "SKEW" ->
          Scenario(
            name = "SKEW",
            description =
              "Enable AQE skew-join optimization for a highly skewed fact dataset.",
            staticBroadcastThreshold = -1,
            adaptiveBroadcastThreshold = -1,
            hint = Some("MERGE"),
            skewJoinEnabled = true
          )
      )

    val scenario: Scenario =
      scenarios.getOrElse(
        scenarioName,
        throw new IllegalArgumentException(
          "Valid scenarios: " +
            scenarios.keys.toSeq.sorted.mkString(", ")
        )
      )

    // -----------------------------------------------------------------------
    // 3. Create Spark session
    // -----------------------------------------------------------------------

    val spark: SparkSession =
      EnterpriseSparkSession.create(
        s"Production Join Optimization - ${scenario.name}",
        "dev"
      )

    // -----------------------------------------------------------------------
    // 4. Configure common Spark settings
    // -----------------------------------------------------------------------

    // Keep shuffle parallelism consistent across scenarios.
    //
    // This makes the physical join strategy and AQE behavior easier to
    // compare because the baseline shuffle configuration remains constant.
    spark.conf.set(
      "spark.sql.shuffle.partitions",
      shufflePartitions.toString
    )

    // AQE is enabled for all capstone scenarios.
    //
    // Some scenarios use AQE specifically for join conversion or skew
    // handling, while the explicit-hint scenarios allow us to observe how
    // AQE interacts with a requested strategy.
    spark.conf.set(
      "spark.sql.adaptive.enabled",
      "true"
    )

    // Configure the static broadcast threshold for the scenario.
    spark.conf.set(
      "spark.sql.autoBroadcastJoinThreshold",
      scenario.staticBroadcastThreshold.toString
    )

    // Configure the AQE runtime broadcast threshold.
    //
    // NATURAL uses this setting to allow AQE to convert an initial
    // shuffle-based join into a broadcast join after runtime statistics
    // are available.
    spark.conf.set(
      "spark.sql.adaptive.autoBroadcastJoinThreshold",
      scenario.adaptiveBroadcastThreshold.toString
    )

    // Disable partition coalescing so that the experiment focuses on
    // join strategy and skew behavior rather than AQE partition coalescing.
    spark.conf.set(
      "spark.sql.adaptive.coalescePartitions.enabled",
      "false"
    )

    // Enable or disable AQE skew handling according to the scenario.
    spark.conf.set(
      "spark.sql.adaptive.skewJoin.enabled",
      scenario.skewJoinEnabled.toString
    )

    // -----------------------------------------------------------------------
    // 5. Configure additional skew settings
    // -----------------------------------------------------------------------
    //
    // These settings are only relevant to the SKEW scenario.
    //
    // The deliberately low byte threshold makes it easier for the local
    // experiment to demonstrate AQE skew detection with the generated
    // workload.

    if (scenario.skewJoinEnabled) {

      spark.conf.set(
        "spark.sql.adaptive.skewJoin.skewedPartitionFactor",
        "2.0"
      )

      spark.conf.set(
        "spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes",
        "1048576"
      )

      // Ask Spark to optimize a skewed join when the skew conditions
      // identified by AQE make the optimization applicable.
      spark.conf.set(
        "spark.sql.adaptive.forceOptimizeSkewedJoin",
        "true"
      )
    }

    // -----------------------------------------------------------------------
    // 6. Create the fact dataset
    // -----------------------------------------------------------------------
    //
    // Normal scenarios:
    //
    //   customer_id = id % 100000
    //
    // This distributes the 10M fact rows relatively evenly across the
    // customer keys.
    //
    // SKEW scenario:
    //
    //   9M rows -> customer_id = 0
    //   1M rows -> distributed across many other keys
    //
    // This creates an intentionally hot key so that AQE skew handling
    // can be investigated.

    val factDf: DataFrame =
      if (scenario.skewJoinEnabled) {

        spark.range(0, 10000000)
          .select(
            col("id").alias("transaction_id"),

            when(
              col("id") < 9000000,
              lit(0L)
            )
              .otherwise(
                (col("id") % 99999) + 1
              )
              .alias("customer_id"),

            (col("id") % 1000).alias("amount")
          )

      } else {

        spark.range(0, 10000000)
          .select(
            col("id").alias("transaction_id"),
            (col("id") % 100000).alias("customer_id"),
            (col("id") % 1000).alias("amount")
          )
      }

    // -----------------------------------------------------------------------
    // 7. Create the customer dimension
    // -----------------------------------------------------------------------
    //
    // The 100K-row dimension is deliberately much smaller than the
    // 10M-row fact dataset.
    //
    // This makes it a natural candidate for broadcast or hash build-side
    // selection in the appropriate scenarios.

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
    // 8. Apply scenario-specific join hints
    // -----------------------------------------------------------------------
    //
    // NATURAL:
    //   No hint. Spark/AQE is allowed to make the decision.
    //
    // BROADCAST:
    //   Broadcast the smaller customer dimension.
    //
    // MERGE:
    //   Request SortMergeJoin.
    //
    // SHUFFLE_HASH:
    //   Request ShuffledHashJoin.
    //
    // SKEW:
    //   Request SortMergeJoin so that AQE skew handling can operate on the
    //   shuffle-based join.

    val factWithHint: DataFrame =
      scenario.hint match {

        case Some("MERGE") =>
          factDf.hint("MERGE")

        case Some("SHUFFLE_HASH") =>
          factDf.hint("SHUFFLE_HASH")

        case _ =>
          factDf
      }

    val customerWithHint: DataFrame =
      scenario.hint match {

        case Some("BROADCAST") =>
          customerDf.hint("BROADCAST")

        case Some("MERGE") =>
          customerDf.hint("MERGE")

        case Some("SHUFFLE_HASH") =>
          customerDf.hint("SHUFFLE_HASH")

        case _ =>
          customerDf
      }

    // -----------------------------------------------------------------------
    // 9. Define the join
    // -----------------------------------------------------------------------
    //
    // This is still a lazy transformation.
    //
    // No join data has been processed until the terminal action below.

    val joinedDf =
      factWithHint.join(
        customerWithHint,
        Seq("customer_id"),
        "inner"
      )

    // -----------------------------------------------------------------------
    // 10. Print experiment configuration
    // -----------------------------------------------------------------------

    println()
    println("====================================================")
    println(
      s"MODULE 1.6.12 - PRODUCTION JOIN OPTIMIZATION | " +
        s"${scenario.name}"
    )
    println("====================================================")

    println()
    println("SCENARIO")
    println("----------------------------------------------------")
    println(s"Name = ${scenario.name}")
    println(s"Description = ${scenario.description}")

    println()
    println("CONFIGURATION")
    println("----------------------------------------------------")
    println(s"Shuffle partitions = $shufflePartitions")
    println(
      "Static broadcast threshold = " +
        s"${scenario.staticBroadcastThreshold} bytes"
    )
    println(
      "Adaptive broadcast threshold = " +
        s"${scenario.adaptiveBroadcastThreshold} bytes"
    )
    println(
      s"Join hint = ${scenario.hint.getOrElse("NONE")}"
    )
    println(
      s"AQE skew join enabled = ${scenario.skewJoinEnabled}"
    )

    // -----------------------------------------------------------------------
    // 11. Inspect the initial physical plan
    // -----------------------------------------------------------------------
    //
    // Before execution, AQE normally exposes the initial physical plan.
    //
    // This is particularly important for NATURAL:
    //
    //   Initial:
    //       SortMergeJoin
    //
    //   Runtime:
    //       Shuffle stages produce statistics
    //
    //   Final:
    //       BroadcastHashJoin
    //
    // This demonstrates why the initial plan and final adaptive plan
    // must not be treated as the same thing.

    println()
    println("====================================================")
    println("INITIAL PHYSICAL PLAN")
    println("====================================================")

    joinedDf.explain("formatted")

    // -----------------------------------------------------------------------
    // 12. Execute the complete join
    // -----------------------------------------------------------------------
    //
    // foreachPartition is used as the terminal action.
    //
    // Every result row is consumed so that the complete join executes.
    //
    // The timer measures this materialization from the driver perspective.
    // It should be used as supporting evidence, not as a universal
    // production performance benchmark.

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
    // 13. Inspect the final executed plan
    // -----------------------------------------------------------------------
    //
    // queryExecution.executedPlan contains the executed adaptive plan.
    //
    // We inspect it after execution to identify the strategy Spark actually
    // used at runtime.
    //
    // Possible indicators:
    //
    //   BroadcastHashJoin
    //   SortMergeJoin
    //   ShuffledHashJoin
    //   skew=true
    //
    // This is especially important for NATURAL because the final strategy
    // can differ from the initial plan.

    val executedPlan =
      joinedDf.queryExecution.executedPlan.toString

    val isFinalAdaptivePlan =
      executedPlan.contains("isFinalPlan=true")

    val hasBroadcastHashJoin =
      executedPlan.contains("BroadcastHashJoin")

    val hasSortMergeJoin =
      executedPlan.contains("SortMergeJoin")

    val hasShuffledHashJoin =
      executedPlan.contains("ShuffledHashJoin")

    val hasSkewJoin =
      executedPlan.contains("skew=true")

    // Translate physical-plan evidence into a concise engineering label.

    val finalJoinStrategy: String =
      if (hasBroadcastHashJoin) {
        "BROADCAST HASH JOIN"
      } else if (hasShuffledHashJoin) {
        "SHUFFLED HASH JOIN"
      } else if (hasSortMergeJoin && hasSkewJoin) {
        "SORT MERGE JOIN + AQE SKEW"
      } else if (hasSortMergeJoin) {
        "SORT MERGE JOIN"
      } else {
        "UNKNOWN"
      }

    // -----------------------------------------------------------------------
    // 14. Print execution summary
    // -----------------------------------------------------------------------

    println()
    println("====================================================")
    println("EXECUTION SUMMARY")
    println("====================================================")

    println(
      s"Final adaptive plan = $isFinalAdaptivePlan"
    )

    println(
      s"Join strategy = $finalJoinStrategy"
    )

    println(
      "Rows joined = 10,000,000"
    )

    println(
      f"Execution time = $seconds%.3f s"
    )

    println(
      s"AQE skew detected = $hasSkewJoin"
    )

    // -----------------------------------------------------------------------
    // 15. Print final adaptive plan
    // -----------------------------------------------------------------------
    //
    // The final plan is the most important evidence for understanding
    // what AQE actually executed.
    //
    // Compare this plan with the initial plan printed above.
    //
    // Examples:
    //
    //   NATURAL:
    //       Initial SMJ -> Final BHJ
    //
    //   SKEW:
    //       Initial SMJ -> Final SMJ(skew=true)
    //
    // This demonstrates that AQE is a runtime optimization layer rather
    // than simply another static join strategy.

    println()
    println("====================================================")
    println("FINAL ADAPTIVE PHYSICAL PLAN")
    println("====================================================")

    joinedDf.explain("formatted")

    // -----------------------------------------------------------------------
    // 16. Spark UI guidance
    // -----------------------------------------------------------------------
    //
    // Physical plans explain the execution structure.
    //
    // Spark UI metrics explain the runtime cost.
    //
    // Inspect:
    //
    //   NATURAL
    //       - shuffle stages
    //       - runtime broadcast
    //       - final BHJ
    //
    //   BROADCAST
    //       - broadcast size
    //       - broadcast build/collect time
    //       - absence of fact-side join shuffle
    //
    //   MERGE
    //       - shuffle read/write
    //       - sort time
    //       - spill
    //
    //   SHUFFLE_HASH
    //       - shuffle volume
    //       - hash build time
    //       - memory/spill behavior
    //
    //   SKEW
    //       - skewed partition
    //       - AQE shuffle reads
    //       - number of skew splits
    //       - task-duration imbalance
    //
    // Do not use execution time alone to explain why a strategy is better.

    println()
    println("====================================================")
    println("SPARK UI")
    println("====================================================")

    println("http://localhost:4040")
    println(
      "Use Spark UI for shuffle, sort/hash/broadcast and task metrics."
    )

    // -----------------------------------------------------------------------
    // 17. Optional UI inspection pause
    // -----------------------------------------------------------------------
    //
    // The optional pause keeps the application alive so that the local
    // Spark UI can be inspected before the Spark session is stopped.

    if (uiPauseSeconds > 0) {

      println()
      println(
        s"Keeping application alive for " +
          s"$uiPauseSeconds seconds for Spark UI inspection..."
      )

      Thread.sleep(
        uiPauseSeconds * 1000L
      )
    }

    // -----------------------------------------------------------------------
    // 18. Clean shutdown
    // -----------------------------------------------------------------------

    spark.stop()
  }
}
