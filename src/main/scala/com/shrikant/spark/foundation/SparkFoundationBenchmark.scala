package com.shrikant.spark.foundation

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions._

/**
 * Benchmark methodology:
 *
 * - Dataset is materialized and cached before scenarios run.
 * - Scenario A and C consume every output row using toLocalIterator().
 * - Scenario B uses count() to force aggregation execution.
 * - Timings represent end-to-end query execution including result consumption.
 * - Results are machine-specific because the benchmark runs with local[*].
 */
object SparkFoundationBenchmark {

  private val DefaultRowCount = 10000000L
  private val DefaultCustomerCount = 1000000L
  private val ShufflePartitions = 8

  def main(args: Array[String]): Unit = {

    val rowCount =
      if (args.nonEmpty) args(0).toLong else DefaultRowCount

    val customerCount =
      if (args.length > 1) args(1).toLong else DefaultCustomerCount

    val shufflePartitions =
      if (args.length > 2) args(2).toInt else ShufflePartitions

    val uiPause = args.contains("--ui-pause")

    // --------------------------------------------------
    // Input validation
    // --------------------------------------------------

    require(rowCount > 0, "rowCount must be > 0")
    require(customerCount > 0, "customerCount must be > 0")
    require(shufflePartitions > 0, "shufflePartitions must be > 0")

    val spark = SparkSession.builder()
      .appName("Module 1.2 - Spark Foundation Benchmark")
      .master("local[*]")
      .config("spark.sql.shuffle.partitions", shufflePartitions)
      .config("spark.sql.adaptive.enabled", "true")
      .config("spark.ui.enabled", "true")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    println()
    println("====================================================")
    println("SPARK FOUNDATION PERFORMANCE BENCHMARK")
    println("====================================================")
    println(s"Rows              : $rowCount")
    println(s"Customers         : $customerCount")
    println(s"Shuffle partitions: $shufflePartitions")
    println(s"Spark version     : ${spark.version}")
    println(s"Java version      : ${System.getProperty("java.version")}")
    println("====================================================")

    // --------------------------------------------------
    // Dataset generation
    // --------------------------------------------------

    println()
    println("Generating benchmark dataset...")

    val generationStart = System.nanoTime()

    val transactions = spark.range(rowCount)
      .select(
        col("id").alias("transaction_id"),

        (pmod(col("id"), lit(customerCount)) + lit(1))
          .cast("string")
          .alias("customer_id"),

        when(pmod(col("id"), lit(5)) === 0, "Pune")
          .when(pmod(col("id"), lit(5)) === 1, "Mumbai")
          .when(pmod(col("id"), lit(5)) === 2, "Delhi")
          .when(pmod(col("id"), lit(5)) === 3, "Bangalore")
          .otherwise("Hyderabad")
          .alias("city"),

        (pmod(col("id") * lit(7919L), lit(990000L)) + lit(10000L))
          .cast("double")
          .alias("amount")
      )
      .cache()

    val generatedRows = transactions.count()

    val generationTime =
      (System.nanoTime() - generationStart) / 1e9

    println(s"Generated rows: $generatedRows")
    println(f"Dataset materialization time: $generationTime%.2f seconds")

    // --------------------------------------------------
    // Scenario A
    // GROUP BY + ORDER BY
    // --------------------------------------------------

    println()
    println("====================================================")
    println("SCENARIO A: GROUP BY + ORDER BY")
    println("====================================================")

    val baseline = transactions
      .groupBy("customer_id")
      .agg(
        sum("amount").alias("total_amount"),
        count("*").alias("transaction_count")
      )
      .orderBy(desc("total_amount"))

    val scenarioAStart = System.nanoTime()

    val scenarioAResult = consumeOrderedRows(baseline)

    val scenarioATime =
      (System.nanoTime() - scenarioAStart) / 1e9

    println(s"Result rows: ${scenarioAResult._1}")
    println(f"Checksum: ${scenarioAResult._2}%.2f")
    println(f"Execution time: $scenarioATime%.2f seconds")

    println()
    println("--- SCENARIO A PHYSICAL PLAN ---")
    baseline.explain(true)

    if (uiPause) {
      pauseForUi("SCENARIO A")
    }

    // --------------------------------------------------
    // Scenario B
    // GROUP BY ONLY
    // --------------------------------------------------

    println()
    println("====================================================")
    println("SCENARIO B: GROUP BY ONLY")
    println("====================================================")

    val aggregationOnly = transactions
      .groupBy("customer_id")
      .agg(
        sum("amount").alias("total_amount"),
        count("*").alias("transaction_count")
      )

    val scenarioBStart = System.nanoTime()

    val scenarioBResult = aggregationOnly.count()

    val scenarioBTime =
      (System.nanoTime() - scenarioBStart) / 1e9

    println(s"Result rows: $scenarioBResult")
    println(f"Execution time: $scenarioBTime%.2f seconds")

    println()
    println("--- SCENARIO B PHYSICAL PLAN ---")
    aggregationOnly.explain(true)

    if (uiPause) {
      pauseForUi("SCENARIO B")
    }

    // --------------------------------------------------
    // Scenario C
    // GROUP BY + ORDER BY + LIMIT 100
    // --------------------------------------------------

    println()
    println("====================================================")
    println("SCENARIO C: GROUP BY + ORDER BY + LIMIT 100")
    println("====================================================")

    val topN = transactions
      .groupBy("customer_id")
      .agg(
        sum("amount").alias("total_amount"),
        count("*").alias("transaction_count")
      )
      .orderBy(desc("total_amount"))
      .limit(100)

    val scenarioCStart = System.nanoTime()

    val scenarioCResult = consumeOrderedRows(topN)

    val scenarioCTime =
      (System.nanoTime() - scenarioCStart) / 1e9

    println(s"Result rows: ${scenarioCResult._1}")
    println(f"Checksum: ${scenarioCResult._2}%.2f")
    println(f"Execution time: $scenarioCTime%.2f seconds")

    println()
    println("--- SCENARIO C PHYSICAL PLAN ---")
    topN.explain(true)

    if (uiPause) {
      pauseForUi("SCENARIO C")
    }

    // --------------------------------------------------
    // Benchmark summary
    // --------------------------------------------------

    val topNVsFullSort = scenarioCTime / scenarioATime
    val orderedPipelineVsAggregation = scenarioATime / scenarioBTime

    println()
    println("====================================================")
    println("BENCHMARK SUMMARY")
    println("====================================================")

    println(f"Group By + Order By          : $scenarioATime%.2f sec")
    println(f"Group By Only                : $scenarioBTime%.2f sec")
    println(f"Group By + Top100            : $scenarioCTime%.2f sec")
    println(f"Top-100 vs Full Sort         : $topNVsFullSort%.2fx")
    println(f"Ordered Pipeline vs Aggregation: $orderedPipelineVsAggregation%.2fx")

    println()
    println("Spark UI: http://localhost:4040")
    println()

    spark.stop()
  }

  /**
   * Consumes every row from the DataFrame.
   *
   * This is intentionally used for ordered scenarios so that
   * Spark must actually produce and consume the ordered result.
   *
   * Returns:
   *   (number of rows consumed, checksum)
   */
  private def consumeOrderedRows(df: DataFrame): (Long, Double) = {

    val iterator = df.toLocalIterator()

    var rowCount = 0L
    var checksum = 0.0

    while (iterator.hasNext) {
      val row: Row = iterator.next()

      checksum += row.getAs[Double]("total_amount")
      rowCount += 1
    }

    (rowCount, checksum)
  }

  private def pauseForUi(scenario: String): Unit = {
    println()
    println("====================================================")
    println(s"SPARK UI PAUSE - $scenario")
    println("====================================================")
    println("Spark UI: http://localhost:4040")
    println()
    println("Review the Spark UI now.")
    println("Press ENTER in this terminal to continue...")
    println()
    scala.io.StdIn.readLine()
  }
}