package com.shrikant.spark.shuffle

import org.apache.spark.scheduler.{SparkListener, SparkListenerTaskEnd}
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable.ArrayBuffer

/**
 * ===========================================================================
 * MODULE 1.8.6 — PUSH-BASED SHUFFLE (SPARK 3.2+)
 * ===========================================================================
 *
 * PURPOSE
 * ===========================================================================
 * Modules 1.8.3 through 1.8.5 explored shuffle volume, memory spill, and
 * serialization/compression trade-offs under standard pull-based shuffles.
 * 
 * This module investigates Push-Based Shuffle, which transforms the traditional
 * pull model (where reducers fetch thousands of small random blocks from
 * upstream executors) into an asynchronous push-and-merge architecture.
 *
 * Specifically, it examines:
 *   1. Configuration setup: Enabling `spark.shuffle.push.based.enabled`.
 *   2. Shuffle fetch characteristics: Sequential vs. random small block reads.
 *   3. Performance impact on shuffle read metrics and stage duration.
 *
 * ===========================================================================
 * CRITICAL DESIGN CONSTRAINT — WHY THIS CLASS RUNS ONE CASE PER JVM
 * ===========================================================================
 * Push-based shuffle is a core transport-layer and block-manager configuration
 * (`spark.shuffle.push.based.enabled`) that must be established during
 * SparkContext initialization. Like memory fraction and serialization settings,
 * it cannot be toggled dynamically post-startup.
 *
 * Therefore, each test runs as an independent JVM process via --case=<LETTER>.
 *
 * ===========================================================================
 * EXPERIMENT MATRIX
 * ===========================================================================
 * Fixed dataset: 10,000,000 rows, 1,000,000 distinct keys, 6 input partitions,
 * 64 shuffle partitions (higher partition count emphasizes shuffle fetch overhead),
 * AQE = false.
 *
 *   A — STANDARD_PULL_SHUFFLE
 *       spark.shuffle.push.based.enabled = false
 *       [Standard pull-based baseline control]
 *
 *   B — PUSH_BASED_SHUFFLE
 *       spark.shuffle.push.based.enabled = true
 *       [Enables asynchronous push-based shuffle merging]
 *
 * ===========================================================================
 * EXECUTION
 * ===========================================================================
 * ./gradlew runShufflePushBasedExperiment --args="--case=A --ui-pause=120"
 * ./gradlew runShufflePushBasedExperiment --args="--case=B"
 * ===========================================================================
 */
object ShufflePushBasedExperiment {

  // =========================================================================
  // DEFAULT CONFIGURATION
  // =========================================================================

  private val DefaultRows = 10000000L
  private val DefaultDistinctKeys = 1000000L
  private val DefaultInputPartitions = 6
  private val DefaultShufflePartitions = 64

  // =========================================================================
  // TASK-LEVEL METRICS
  // =========================================================================

  private final case class TaskPushMetrics(
      stageId: Int,
      taskId: Long,
      partitionId: Int,
      shuffleReadBytes: Long,
      shuffleReadRecords: Long,
      shuffleWriteBytes: Long,
      shuffleWriteRecords: Long,
      fetchWaitTimeMs: Long,
      executorRunTimeMs: Long
  )

  // =========================================================================
  // SPARK LISTENER
  // =========================================================================

  private final class PushMetricsListener extends SparkListener {

    private val metricsBuffer = ArrayBuffer.empty[TaskPushMetrics]

    override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = synchronized {
      val metrics = taskEnd.taskMetrics

      if (metrics != null) {
        val read = metrics.shuffleReadMetrics
        val write = metrics.shuffleWriteMetrics

        metricsBuffer += TaskPushMetrics(
          stageId = taskEnd.stageId,
          taskId = if (taskEnd.taskInfo != null) taskEnd.taskInfo.taskId else -1L,
          partitionId = if (taskEnd.taskInfo != null) taskEnd.taskInfo.index else -1,
          shuffleReadBytes = if (read != null) read.totalBytesRead else 0L,
          shuffleReadRecords = if (read != null) read.recordsRead else 0L,
          shuffleWriteBytes = if (write != null) write.bytesWritten else 0L,
          shuffleWriteRecords = if (write != null) write.recordsWritten else 0L,
          fetchWaitTimeMs = if (read != null) read.fetchWaitTime else 0L,
          executorRunTimeMs = metrics.executorRunTime
        )
      }
    }

    def snapshot(): Seq[TaskPushMetrics] = synchronized { metricsBuffer.toVector }

    def clear(): Unit = synchronized { metricsBuffer.clear() }
  }

  // =========================================================================
  // CASE DEFINITION
  // =========================================================================

  private final case class PushCase(
      label: String,
      description: String,
      pushEnabled: Boolean
  )

  private val CaseMatrix: Map[String, PushCase] = Map(

    "A" -> PushCase(
      label = "A — STANDARD PULL-BASED SHUFFLE (BASELINE)",
      description = "Standard Spark pull-based shuffle where reducers fetch small random blocks from mappers.",
      pushEnabled = false
    ),

    "B" -> PushCase(
      label = "B — PUSH-BASED SHUFFLE ENABLED",
      description = "Enables push-based shuffle where map outputs are asynchronously pushed and merged server-side.",
      pushEnabled = true
    )
  )

  // =========================================================================
  // MAIN
  // =========================================================================

  def main(args: Array[String]): Unit = {
    val config = parseArguments(args)
    val testCase = CaseMatrix(config.caseLabel)

    printHeader(config, testCase)

    val spark = SparkSession.builder()
      .appName(s"Module 1.8.6 - Push-Based Shuffle - Case ${config.caseLabel}")
      .master("local[2]")
      .config("spark.shuffle.push.based.enabled", testCase.pushEnabled)
      .config("spark.sql.shuffle.partitions", DefaultShufflePartitions)
      .config("spark.sql.adaptive.enabled", false)
      .getOrCreate()

    val listener = new PushMetricsListener()
    spark.sparkContext.addSparkListener(listener)

    var input: DataFrame = null

    try {
      println(s"Effective spark.shuffle.push.based.enabled : ${spark.sparkContext.getConf.get("spark.shuffle.push.based.enabled")}")
      println()

      input = createInputDataset(
        spark = spark,
        rows = config.rows,
        distinctKeys = config.distinctKeys,
        inputPartitions = config.inputPartitions
      )

      println("INPUT DATASET")
      println("============================================================")
      println(s"Rows               : ${config.rows}")
      println(s"Distinct customers : ${config.distinctKeys}")
      println(s"Input partitions   : ${input.rdd.getNumPartitions}")
      println()

      runCase(
        spark = spark,
        input = input,
        testCase = testCase,
        listener = listener
      )

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
      println(s"MODULE 1.8.6 CASE ${config.caseLabel} COMPLETED")
      println("============================================================")
    }
  }

  // =========================================================================
  // INPUT DATASET
  // =========================================================================

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

  // =========================================================================
  // CASE EXECUTION
  // =========================================================================

  private def runCase(
      spark: SparkSession,
      input: DataFrame,
      testCase: PushCase,
      listener: PushMetricsListener
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

    println("----- PHYSICAL PLAN -----")
    println("------------------------------------------------------------")
    result.explain(mode = "formatted")

    listener.clear()
    val start = System.nanoTime()
    val resultCount = result.count()
    val elapsedSeconds = (System.nanoTime() - start) / 1000000000.0

    require(resultCount > 0, s"Experiment ${testCase.label} returned zero rows.")

    println()
    println("RESULT")
    println("------------------------------------------------------------")
    println(s"Result rows : $resultCount")
    println(f"Runtime     : $elapsedSeconds%.3f seconds")

    printPushMetrics(listener.snapshot())
  }

  // =========================================================================
  // METRICS REPORTING
  // =========================================================================

  private def printPushMetrics(metrics: Seq[TaskPushMetrics]): Unit = {
    println()
    println("----- PUSH-BASED SHUFFLE METRICS -----")
    println("------------------------------------------------------------")

    if (metrics.isEmpty) {
      println("No task metrics were captured.")
      return
    }

    val totalShuffleWrite = metrics.map(_.shuffleWriteBytes).sum
    val totalShuffleRead = metrics.map(_.shuffleReadBytes).sum
    val totalFetchWaitTimeMs = metrics.map(_.fetchWaitTimeMs).sum
    val totalExecutorRunTimeMs = metrics.map(_.executorRunTimeMs).sum

    println(s"Shuffle write bytes (total)  : ${formatBytes(totalShuffleWrite)}")
    println(s"Shuffle read bytes (total)   : ${formatBytes(totalShuffleRead)}")
    println(s"Sum of fetch wait time       : ${totalFetchWaitTimeMs} ms")
    println(s"Sum of task executor runtime : ${totalExecutorRunTimeMs} ms")
  }

  // =========================================================================
  // BYTE FORMATTING
  // =========================================================================

  private def formatBytes(bytes: Long): String = {
    if (bytes < 1024L) s"$bytes B"
    else if (bytes < 1024L * 1024L) f"${bytes / 1024.0}%.2f KiB"
    else if (bytes < 1024L * 1024L * 1024L) f"${bytes / (1024.0 * 1024.0)}%.2f MiB"
    else f"${bytes / (1024.0 * 1024.0 * 1024.0)}%.2f GiB"
  }

  // =========================================================================
  // ARGUMENT PARSING & CONFIG
  // =========================================================================

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
          "Missing required --case=<A|B> argument for Module 1.8.6."
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

  private def printHeader(config: ExperimentConfig, testCase: PushCase): Unit = {
    println()
    println("============================================================")
    println("MODULE 1.8.6 — PUSH-BASED SHUFFLE")
    println("============================================================")
    println(s"Case              : ${config.caseLabel} —${testCase.label}")
    println(s"Rows              : ${config.rows}")
    println(s"Distinct keys     : ${config.distinctKeys}")
    println(s"Input partitions  : ${config.inputPartitions}")
    println(s"Push shuffle enabled: ${testCase.pushEnabled}")
    println(s"UI pause          : ${config.uiPauseSeconds} seconds")
    println("============================================================")
  }
}