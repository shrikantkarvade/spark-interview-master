package com.shrikant.spark.shuffle

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * ===========================================================================
 * MODULE 1.8.7 — ADAPTIVE QUERY EXECUTION (AQE) SHUFFLE OPTIMIZATIONS
 * ===========================================================================
 *
 * PURPOSE
 * ===========================================================================
 * This module investigates Spark's Adaptive Query Execution (AQE) framework,
 * focusing on dynamic partition coalescing and runtime plan adjustments.
 *
 * EXPERIMENT MATRIX
 * ===========================================================================
 * Fixed dataset: 10,000,000 rows, 1,000,000 distinct keys, 6 input partitions,
 * with shuffle partitions initialized to a high default count (200) to observe
 * AQE's dynamic partition coalescing behavior.
 *
 *   A — AQE DISABLED (STATIC PLANNING)
 *       spark.sql.adaptive.enabled = false
 *       spark.sql.shuffle.partitions = 200
 *
 *   B — AQE ENABLED (DYNAMIC RE-OPTIMIZATION)
 *       spark.sql.adaptive.enabled = true
 *       spark.sql.shuffle.partitions = 200
 *
 * ===========================================================================
 */
object ShuffleAQEExperiment {

  private val DefaultRows = 10000000L
  private val DefaultDistinctKeys = 1000000L
  private val DefaultInputPartitions = 6
  private val InitialShufflePartitions = 200

  private final case class AQECase(
      label: String,
      description: String,
      aqeEnabled: Boolean
  )

  private val CaseMatrix: Map[String, AQECase] = Map(
    "A" -> AQECase(
      label = "A — AQE DISABLED (STATIC PLANNING)",
      description = "Standard execution using static partition count (200) without runtime re-optimization.",
      aqeEnabled = false
    ),
    "B" -> AQECase(
      label = "B — AQE ENABLED (DYNAMIC COALESCING)",
      description = "Enables AQE to dynamically coalesce small shuffle partitions based on runtime statistics.",
      aqeEnabled = true
    )
  )

  def main(args: Array[String]): Unit = {
    val config = parseArguments(args)
    val testCase = CaseMatrix(config.caseLabel)

    printHeader(config, testCase)

    val spark = SparkSession.builder()
      .appName(s"Module 1.8.7 - AQE Optimization - Case ${config.caseLabel}")
      .master("local[2]")
      .config("spark.sql.adaptive.enabled", testCase.aqeEnabled)
      .config("spark.sql.shuffle.partitions", InitialShufflePartitions)
      .getOrCreate()

    var input: DataFrame = null

    try {
      println(s"Effective spark.sql.adaptive.enabled : ${spark.sparkContext.getConf.get("spark.sql.adaptive.enabled")}")
      println(s"Initial spark.sql.shuffle.partitions : ${spark.sparkContext.getConf.get("spark.sql.shuffle.partitions")}")
      println()

      input = createInputDataset(
        spark = spark,
        rows = config.rows,
        distinctKeys = config.distinctKeys,
        inputPartitions = config.inputPartitions
      )

      runCase(spark = spark, input = input, testCase = testCase)

      if (config.uiPauseSeconds > 0) {
        println()
        println("============================================================")
        println("SPARK UI PAUSE")
        println("============================================================")
        println(s"Spark UI remains available for ${config.uiPauseSeconds} seconds.")
        println("Open: http://localhost:4040")
        println("============================================================")
        Thread.sleep(config.uiPauseSeconds.toLong * 1000L)
      }

    } finally {
      if (input != null) {
        try { input.unpersist(blocking = false) } catch { case _: Throwable => }
      }
      spark.stop()
      println()
      println("============================================================")
      println(s"MODULE 1.8.7 CASE ${config.caseLabel} COMPLETED")
      println("============================================================")
    }
  }

  private def createInputDataset(
      spark: SparkSession,
      rows: Long,
      distinctKeys: Long,
      inputPartitions: Int
  ): DataFrame = {
    import spark.implicits._

    spark
      .range(start = 0L, end = rows, step = 1L, numPartitions = inputPartitions)
      .select(
        $"id".as("transaction_id"),
        ($"id" % distinctKeys).as("customer_id"),
        (($"id" * 31L) % 100000L).as("product_id"),
        (($"id" * 17L) % 100000L).cast("double").as("amount")
      )
  }

  private def runCase(
      spark: SparkSession,
      input: DataFrame,
      testCase: AQECase
  ): Unit = {
    import org.apache.spark.sql.functions._

    println("################################################################")
    println(s"# EXPERIMENT: ${testCase.label}")
    println("################################################################")
    println()
    println("DESCRIPTION")
    println("------------------------------------------------------------")
    println(testCase.description)
    println()

    val grouped = input.groupBy("customer_id")
    val result = grouped.agg(
      count(lit(1)).as("transaction_count"),
      sum("amount").as("total_amount"),
      avg("amount").as("avg_amount")
    )

    println("----- PHYSICAL PLAN (QUOTED / EXPLAIN) -----")
    println("------------------------------------------------------------")
    result.explain(mode = "extended")

    val start = System.nanoTime()
    val resultCount = result.count()
    val elapsedSeconds = (System.nanoTime() - start) / 1000000000.0

    require(resultCount > 0, s"Experiment ${testCase.label} returned zero rows.")

    println()
    println("RESULT & PERFORMANCE")
    println("------------------------------------------------------------")
    println(s"Result rows : $resultCount")
    println(f"Runtime     : $elapsedSeconds%.3f seconds")
  }

  final case class ExperimentConfig(
      caseLabel: String,
      rows: Long,
      distinctKeys: Long,
      inputPartitions: Int,
      uiPauseSeconds: Int
  )

  private def parseArguments(args: Array[String]): ExperimentConfig = {
    val caseLabel = args
      .find(_.startsWith("--case="))
      .map(_.stripPrefix("--case=").toUpperCase)
      .getOrElse(
        throw new IllegalArgumentException(
          "Missing required --case=<A|B> argument for Module 1.8.7."
        )
      )

    require(CaseMatrix.contains(caseLabel), s"Unknown case '$caseLabel'. Valid: ${CaseMatrix.keys.toSeq.sorted.mkString(", ")}")

    val positional = args.filterNot(_.startsWith("--"))
    val rows = positional.headOption.map(_.toLong).getOrElse(DefaultRows)
    val distinctKeys = positional.drop(1).headOption.map(_.toLong).getOrElse(DefaultDistinctKeys)
    val inputPartitions = positional.drop(2).headOption.map(_.toInt).getOrElse(DefaultInputPartitions)

    val uiPauseSeconds = args
      .find(_.startsWith("--ui-pause="))
      .map(_.stripPrefix("--ui-pause=").toInt)
      .getOrElse(0)

    ExperimentConfig(
      caseLabel = caseLabel,
      rows = rows,
      distinctKeys = distinctKeys,
      inputPartitions = inputPartitions,
      uiPauseSeconds = uiPauseSeconds
    )
  }

  private def printHeader(config: ExperimentConfig, testCase: AQECase): Unit = {
    println()
    println("============================================================")
    println("MODULE 1.8.7 — ADAPTIVE QUERY EXECUTION (AQE)")
    println("============================================================")
    println(s"Case              : ${config.caseLabel} — ${testCase.label}")
    println(s"Rows              : ${config.rows}")
    println(s"Distinct keys     : ${config.distinctKeys}")
    println(s"Input partitions  : ${config.inputPartitions}")
    println(s"AQE enabled       : ${testCase.aqeEnabled}")
    println(s"UI pause          : ${config.uiPauseSeconds} seconds")
    println("============================================================")
  }
}