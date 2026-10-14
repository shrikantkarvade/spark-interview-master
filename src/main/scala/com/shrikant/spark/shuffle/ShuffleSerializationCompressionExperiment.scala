package com.shrikant.spark.shuffle

import org.apache.spark.scheduler.{SparkListener, SparkListenerTaskEnd}
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable.ArrayBuffer

/**
 * ===========================================================================
 * MODULE 1.8.5 — SERIALIZATION & COMPRESSION
 * ===========================================================================
 *
 * PURPOSE
 * ===========================================================================
 * Module 1.8.3 measured shuffle I/O volume and Module 1.8.4 investigated
 * memory pressure and disk spill. This module moves directly to the cost
 * and performance trade-offs of encoding and compressing shuffle data.
 *
 * Specifically, it investigates:
 *   1. Serializer impact: JavaSerializer vs. KryoSerializer.
 *   2. Compression codecs: uncompressed vs. lz4 vs. snappy vs. zstd.
 *   3. The CPU vs. I/O trade-off: balancing serialization/compression CPU
 *      overhead against network and disk transfer volume.
 *
 * ===========================================================================
 * CRITICAL DESIGN CONSTRAINT — WHY THIS CLASS RUNS ONE CASE PER JVM
 * ===========================================================================
 * Similar to Module 1.8.4, critical serialization and compression properties
 * (such as spark.serializer and spark.shuffle.compress / codecs) are bound
 * at SparkContext initialization time. Once the SparkContext is running,
 * changing key serialization parameters or compression codecs dynamically
 * via spark.conf.set(...) is either ignored or throws configuration errors.
 *
 * Therefore, each test in this module's matrix runs as an independent JVM
 * process, taking a single --case=<LETTER> argument.
 *
 * ===========================================================================
 * EXPERIMENT MATRIX
 * ===========================================================================
 * Fixed dataset: 10,000,000 rows, 1,000,000 distinct keys, 6 input partitions,
 * 24 shuffle partitions, AQE = false.
 *
 *   A — DEFAULT_JAVA_LZ4
 *       JavaSerializer + spark.shuffle.compress = true (lz4 codec)
 *       [Baseline control]
 *
 *   B — KRYO_LZ4
 *       KryoSerializer + spark.shuffle.compress = true (lz4 codec)
 *       [Measures Kryo serialization efficiency vs Java]
 *
 *   C — DEFAULT_JAVA_UNCOMPRESSED
 *       JavaSerializer + spark.shuffle.compress = false
 *       [Measures raw I/O penalty without compression]
 *
 *   D — KRYO_UNCOMPRESSED
 *       KryoSerializer + spark.shuffle.compress = false
 *       [Measures Kryo footprint without compression]
 *
 *   E — KRYO_ZSTD
 *       KryoSerializer + spark.shuffle.compress = true + zstd codec
 *       [Measures high-compression CPU vs. size trade-off]
 *
 * ===========================================================================
 * EXECUTION
 * ===========================================================================
 * ./gradlew runShuffleSerializationCompressionExperiment --args="--case=A --ui-pause=120"
 * ./gradlew runShuffleSerializationCompressionExperiment --args="--case=B"
 * ./gradlew runShuffleSerializationCompressionExperiment --args="--case=C"
 * ./gradlew runShuffleSerializationCompressionExperiment --args="--case=D"
 * ./gradlew runShuffleSerializationCompressionExperiment --args="--case=E"
 * ===========================================================================
 */
object ShuffleSerializationCompressionExperiment {

  // =========================================================================
  // DEFAULT CONFIGURATION
  // =========================================================================

  private val DefaultRows = 10000000L
  private val DefaultDistinctKeys = 1000000L
  private val DefaultInputPartitions = 6
  private val DefaultShufflePartitions = 24

  // =========================================================================
  // TASK-LEVEL METRICS
  // =========================================================================

  private final case class TaskSerializationMetrics(
      stageId: Int,
      taskId: Long,
      partitionId: Int,
      shuffleReadBytes: Long,
      shuffleReadRecords: Long,
      shuffleWriteBytes: Long,
      shuffleWriteRecords: Long,
      jvmGCTimeMs: Long,
      executorCpuTimeNs: Long,
      executorRunTimeMs: Long
  )

  // =========================================================================
  // SPARK LISTENER
  // =========================================================================

  private final class SerializationMetricsListener extends SparkListener {

    private val metricsBuffer = ArrayBuffer.empty[TaskSerializationMetrics]

    override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = synchronized {
      val metrics = taskEnd.taskMetrics

      if (metrics != null) {
        val read = metrics.shuffleReadMetrics
        val write = metrics.shuffleWriteMetrics

        metricsBuffer += TaskSerializationMetrics(
          stageId = taskEnd.stageId,
          taskId = if (taskEnd.taskInfo != null) taskEnd.taskInfo.taskId else -1L,
          partitionId = if (taskEnd.taskInfo != null) taskEnd.taskInfo.index else -1,
          shuffleReadBytes = if (read != null) read.totalBytesRead else 0L,
          shuffleReadRecords = if (read != null) read.recordsRead else 0L,
          shuffleWriteBytes = if (write != null) write.bytesWritten else 0L,
          shuffleWriteRecords = if (write != null) write.recordsWritten else 0L,
          jvmGCTimeMs = metrics.jvmGCTime,
          executorCpuTimeNs = metrics.executorCpuTime,
          executorRunTimeMs = metrics.executorRunTime
        )
      }
    }

    def snapshot(): Seq[TaskSerializationMetrics] = synchronized { metricsBuffer.toVector }

    def clear(): Unit = synchronized { metricsBuffer.clear() }
  }

  // =========================================================================
  // CASE DEFINITION
  // =========================================================================

  private final case class SerializationCase(
      label: String,
      description: String,
      serializer: String,
      compress: Boolean,
      codec: Option[String]
  )

  private val CaseMatrix: Map[String, SerializationCase] = Map(

    "A" -> SerializationCase(
      label = "A — DEFAULT JAVA + LZ4 (BASELINE)",
      description = "JavaSerializer with standard shuffle compression enabled (lz4). Baseline control.",
      serializer = "org.apache.spark.serializer.JavaSerializer",
      compress = true,
      codec = None // Spark default lz4
    ),

    "B" -> SerializationCase(
      label = "B — KRYO + LZ4",
      description = "KryoSerializer with standard shuffle compression enabled (lz4). Measures compact serialization savings.",
      serializer = "org.apache.spark.serializer.KryoSerializer",
      compress = true,
      codec = None
    ),

    "C" -> SerializationCase(
      label = "C — DEFAULT JAVA + UNCOMPRESSED",
      description = "JavaSerializer with shuffle compression disabled (spark.shuffle.compress = false).",
      serializer = "org.apache.spark.serializer.JavaSerializer",
      compress = false,
      codec = None
    ),

    "D" -> SerializationCase(
      label = "D — KRYO + UNCOMPRESSED",
      description = "KryoSerializer with shuffle compression disabled. Isolates pure serialization footprint without codec overhead.",
      serializer = "org.apache.spark.serializer.KryoSerializer",
      compress = false,
      codec = None
    ),

    "E" -> SerializationCase(
      label = "E — KRYO + ZSTD",
      description = "KryoSerializer with high-compression zstd codec enabled. Measures maximum compression trade-off.",
      serializer = "org.apache.spark.serializer.KryoSerializer",
      compress = true,
      codec = Some("zstd")
    )
  )

  // =========================================================================
  // MAIN
  // =========================================================================

  def main(args: Array[String]): Unit = {
    val config = parseArguments(args)
    val testCase = CaseMatrix(config.caseLabel)

    printHeader(config, testCase)

    // Build SparkSession directly to guarantee pre-context setting of serializer & compression
    val builder = SparkSession.builder()
      .appName(s"Module 1.8.5 - Serialization & Compression - Case ${config.caseLabel}")
      .master("local[2]")
      .config("spark.serializer", testCase.serializer)
      .config("spark.shuffle.compress", testCase.compress)
      .config("spark.sql.shuffle.partitions", DefaultShufflePartitions)
      .config("spark.sql.adaptive.enabled", false)

    testCase.codec.foreach { c =>
      builder.config("spark.io.compression.codec", c)
    }

    val spark = builder.getOrCreate()
    val listener = new SerializationMetricsListener()
    spark.sparkContext.addSparkListener(listener)

    var input: DataFrame = null

    try {
      println(s"Effective spark.serializer     : ${spark.sparkContext.getConf.get("spark.serializer")}")
      println(s"Effective spark.shuffle.compress : ${spark.sparkContext.getConf.get("spark.shuffle.compress")}")
      if (testCase.codec.isDefined) {
        println(s"Effective compression codec    : ${spark.sparkContext.getConf.get("spark.io.compression.codec", "default")}")
      }
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
      println(s"MODULE 1.8.5 CASE ${config.caseLabel} COMPLETED")
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
      testCase: SerializationCase,
      listener: SerializationMetricsListener
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

    printSerializationMetrics(listener.snapshot())
  }

  // =========================================================================
  // METRICS REPORTING
  // =========================================================================

  private def printSerializationMetrics(metrics: Seq[TaskSerializationMetrics]): Unit = {
    println()
    println("----- SERIALIZATION & I/O METRICS -----")
    println("------------------------------------------------------------")

    if (metrics.isEmpty) {
      println("No task metrics were captured.")
      return
    }

    val totalShuffleWrite = metrics.map(_.shuffleWriteBytes).sum
    val totalShuffleRead = metrics.map(_.shuffleReadBytes).sum
    val totalExecutorRunTimeMs = metrics.map(_.executorRunTimeMs).sum
    val totalCpuTimeMs = metrics.map(_.executorCpuTimeNs / 1000000L).sum
    val totalGCTimeMs = metrics.map(_.jvmGCTimeMs).sum

    println(s"Shuffle write bytes (total)  : ${formatBytes(totalShuffleWrite)}")
    println(s"Shuffle read bytes (total)   : ${formatBytes(totalShuffleRead)}")
    println(s"Sum of task executor runtime : ${totalExecutorRunTimeMs} ms")
    println(s"Sum of task CPU time         : ${totalCpuTimeMs} ms")
    println(s"Sum of JVM GC time           : ${totalGCTimeMs} ms")

    if (totalExecutorRunTimeMs > 0) {
      val cpuRatio = totalCpuTimeMs.toDouble / totalExecutorRunTimeMs.toDouble * 100.0
      println(f"CPU time as %% of runtime    : $cpuRatio%.2f%%")
    }
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
          "Missing required --case=<A|B|C|D|E> argument for Module 1.8.5."
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

  private def printHeader(config: ExperimentConfig, testCase: SerializationCase): Unit = {
    println()
    println("============================================================")
    println("MODULE 1.8.5 — SERIALIZATION & COMPRESSION")
    println("============================================================")
    println(s"Case              : ${config.caseLabel} — ${testCase.label}")
    println(s"Rows              : ${config.rows}")
    println(s"Distinct keys     : ${config.distinctKeys}")
    println(s"Input partitions  : ${config.inputPartitions}")
    println(s"Serializer        : ${testCase.serializer}")
    println(s"Shuffle compress  : ${testCase.compress}")
    println(s"Compression codec : ${testCase.codec.getOrElse("default (lz4)")}")
    println(s"UI pause          : ${config.uiPauseSeconds} seconds")
    println("============================================================")
  }
}