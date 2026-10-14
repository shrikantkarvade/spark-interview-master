package com.shrikant.spark.shuffle

import com.shrikant.spark.foundation.EnterpriseSparkSession
import org.apache.spark.scheduler.{SparkListener, SparkListenerTaskEnd}
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable.ArrayBuffer

/**
 * Module 1.8.3
 *
 * Shuffle Read / Write Performance
 *
 * ===========================================================================
 * PURPOSE
 * ===========================================================================
 *
 * This experiment investigates Spark shuffle I/O rather than relying only on
 * end-to-end application runtime.
 *
 * The input dataset is deliberately kept constant:
 *
 * Input rows       = 10,000,000
 * Distinct keys    = 1,000,000
 * Input partitions = 6
 *
 * We then vary:
 *
 *   1. Shuffle partition count
 *      2. Aggregation width
 *      3. Grouping-key structure
 *      4. AQE behavior in one controlled validation case
 *
 * The objective is to observe:
 *
 *   - Shuffle write bytes
 *   - Shuffle write records
 *   - Shuffle write time
 *   - Shuffle read bytes
 *   - Local shuffle read bytes
 *   - Remote shuffle read bytes
 *   - Remote shuffle read-to-disk bytes
 *   - Shuffle read records
 *   - Local shuffle blocks
 *   - Remote shuffle blocks
 *   - Fetch wait time
 *   - Disk spill bytes
 *   - Memory spill bytes
 *   - Task-level variation
 *   - Physical-plan changes
 *   - Relationship between configured partitions and actual shuffle I/O
 *
 * ===========================================================================
 * IMPORTANT DESIGN DECISION
 * ===========================================================================
 *
 * AQE is disabled for the primary experiment matrix.
 *
 * Why?
 *
 * AQE can coalesce post-shuffle partitions and therefore make:
 *
 * spark.sql.shuffle.partitions
 *
 * different from the effective number of downstream partitions.
 *
 * The primary matrix therefore establishes controlled evidence first.
 *
 * One separate AQE validation case is executed afterward to demonstrate how
 * AQE can change the effective physical execution.
 *
 * ===========================================================================
 * CONFIGURATION ACCESSOR NOTE (fix for SQL_CONF_NOT_FOUND)
 * ===========================================================================
 *
 * `spark.conf.get(...)` (RuntimeConfig) is backed by SQLConf. It only knows
 * about `spark.sql.*`-registered configs, plus arbitrary keys that were
 * explicitly set via `spark.conf.set(...)` in this session.
 *
 * `spark.shuffle.compress` and `spark.shuffle.spill.compress` are CORE Spark
 * configs (org.apache.spark.internal.config), not SQL configs. They are
 * never registered with SQLConf, so `spark.conf.get("spark.shuffle.compress")`
 * always throws SparkNoSuchElementException / SQL_CONF_NOT_FOUND, regardless
 * of when it's called.
 *
 * The fix is to read these two keys from the underlying SparkConf instead:
 *
 *   spark.sparkContext.getConf.get("spark.shuffle.compress", "true")
 *
 * This is a read-only lookup with a sensible default (Spark's own default
 * for both configs is "true") and works whether or not the value was ever
 * explicitly set.
 *
 * ===========================================================================
 * EXECUTION
 * ===========================================================================
 *
 * Default:
 *
 * ./gradlew runShuffleReadWritePerformanceExperiment
 *
 * Custom input:
 *
 * ./gradlew runShuffleReadWritePerformanceExperiment \
 * --args="10000000 1000000 6"
 *
 * UI pause:
 *
 * ./gradlew runShuffleReadWritePerformanceExperiment \
 * --args="10000000 1000000 6 --ui-pause=120"
 *
 * ===========================================================================
 * EXPECTED ARGUMENTS
 * ===========================================================================
 *
 * Positional:
 *
 *   1. rows
 *      2. distinctKeys
 *      3. inputPartitions
 *
 * Optional:
 *
 * --ui-pause=&lt;seconds&gt;
 *
 * Example:
 *
 * --args="10000000 1000000 6 --ui-pause=120"
 *
 * ===========================================================================
 */
object ShuffleReadWritePerformanceExperiment {

  // =========================================================================
  // DEFAULT CONFIGURATION
  // =========================================================================

  private val DefaultRows = 10000000L
  private val DefaultDistinctKeys = 1000000L
  private val DefaultInputPartitions = 6

  // =========================================================================
  // TASK-LEVEL METRICS
  // =========================================================================

  /**
   * Captures shuffle-related metrics for one completed Spark task.
   *
   * Why task-level metrics?
   *
   * Aggregate application-level numbers tell us how much shuffle occurred,
   * but they do not tell us whether the workload was evenly distributed.
   *
   * Task-level metrics allow us to investigate:
   *
   *   - skew
   *   - uneven shuffle reads
   *   - uneven shuffle writes
   *   - fetch wait concentration
   *   - local vs remote reads
   *   - spill behavior
   */
  private final case class TaskShuffleMetrics(
                                               stageId: Int,
                                               taskId: Long,
                                               partitionId: Int,

                                               shuffleReadBytes: Long,
                                               localReadBytes: Long,
                                               remoteReadBytes: Long,
                                               remoteReadToDiskBytes: Long,
                                               shuffleReadRecords: Long,

                                               localBlocks: Long,
                                               remoteBlocks: Long,

                                               fetchWaitTimeMs: Long,

                                               shuffleWriteBytes: Long,
                                               shuffleWriteRecords: Long,
                                               shuffleWriteTimeMs: Long,

                                               memoryBytesSpilled: Long,
                                               diskBytesSpilled: Long
                                             )

  // =========================================================================
  // SPARK LISTENER
  // =========================================================================

  /**
   * Captures TaskMetrics emitted by Spark.
   *
   * The listener is attached once to the SparkContext.
   *
   * Metrics are cleared before every experiment case so that each experiment
   * has an isolated task-metric collection.
   *
   * The ArrayBuffer is protected with synchronization because Spark listener
   * callbacks may be delivered asynchronously.
   */
  private final class ShuffleMetricsListener extends SparkListener {

    private val metricsBuffer =
      ArrayBuffer.empty[TaskShuffleMetrics]

    override def onTaskEnd(
                            taskEnd: SparkListenerTaskEnd
                          ): Unit = synchronized {

      val metrics = taskEnd.taskMetrics

      if (metrics != null) {

        val read = metrics.shuffleReadMetrics
        val write = metrics.shuffleWriteMetrics

        metricsBuffer += TaskShuffleMetrics(

          stageId = taskEnd.stageId,

          taskId =
            if (taskEnd.taskInfo != null)
              taskEnd.taskInfo.taskId
            else
              -1L,

          partitionId =
            if (taskEnd.taskInfo != null)
              taskEnd.taskInfo.index
            else
              -1,

          shuffleReadBytes =
            if (read != null)
              read.totalBytesRead
            else
              0L,

          localReadBytes =
            if (read != null)
              read.localBytesRead
            else
              0L,

          remoteReadBytes =
            if (read != null)
              read.remoteBytesRead
            else
              0L,

          remoteReadToDiskBytes =
            if (read != null)
              read.remoteBytesReadToDisk
            else
              0L,

          shuffleReadRecords =
            if (read != null)
              read.recordsRead
            else
              0L,

          localBlocks =
            if (read != null)
              read.localBlocksFetched
            else
              0L,

          remoteBlocks =
            if (read != null)
              read.remoteBlocksFetched
            else
              0L,

          fetchWaitTimeMs =
            if (read != null)
              read.fetchWaitTime
            else
              0L,

          shuffleWriteBytes =
            if (write != null)
              write.bytesWritten
            else
              0L,

          shuffleWriteRecords =
            if (write != null)
              write.recordsWritten
            else
              0L,

          shuffleWriteTimeMs =
            if (write != null)
              write.writeTime / 1000000L
            else
              0L,

          memoryBytesSpilled =
            metrics.memoryBytesSpilled,

          diskBytesSpilled =
            metrics.diskBytesSpilled
        )
      }
    }

    /**
     * Returns a snapshot rather than exposing the mutable collection.
     */
    def snapshot(): Seq[TaskShuffleMetrics] =
      synchronized {
        metricsBuffer.toVector
      }

    def clear(): Unit =
      synchronized {
        metricsBuffer.clear()
      }
  }

  // =========================================================================
  // EXPERIMENT DEFINITION
  // =========================================================================

  private final case class ExperimentCase(
                                           name: String,
                                           description: String,
                                           shufflePartitions: Int,
                                           aggregationType: String,
                                           groupingType: String,
                                           aqeEnabled: Boolean = false
                                         )

  /**
   * Primary controlled experiment matrix.
   *
   * Dataset characteristics remain constant.
   *
   * Only shuffle-related workload/configuration changes.
   */
  private def primaryExperimentMatrix: Seq[ExperimentCase] = {

    Seq(

      ExperimentCase(
        name = "A — BASELINE — 6 SHUFFLE PARTITIONS",
        description =
          "1M-key aggregation using the same partition count as the input.",
        shufflePartitions = 6,
        aggregationType = "count",
        groupingType = "customer_id"
      ),

      ExperimentCase(
        name = "B — 24 SHUFFLE PARTITIONS",
        description =
          "Same 1M-key aggregation with four times as many shuffle partitions.",
        shufflePartitions = 24,
        aggregationType = "count",
        groupingType = "customer_id"
      ),

      ExperimentCase(
        name = "C — 64 SHUFFLE PARTITIONS",
        description =
          "Same 1M-key aggregation with substantially smaller shuffle partitions.",
        shufflePartitions = 64,
        aggregationType = "count",
        groupingType = "customer_id"
      ),

      ExperimentCase(
        name = "D — WIDER AGGREGATION",
        description =
          "Customer aggregation with both count and sum(amount).",
        shufflePartitions = 24,
        aggregationType = "count_sum",
        groupingType = "customer_id"
      ),

      ExperimentCase(
        name = "E — TWO-KEY GROUPING",
        description =
          "Aggregation grouped by customer_id and product_id.",
        shufflePartitions = 24,
        aggregationType = "count",
        groupingType = "customer_product"
      ),

      ExperimentCase(
        name = "F — TWO-KEY GROUPING — 64 PARTITIONS",
        description =
          "Two-key aggregation using smaller shuffle partitions.",
        shufflePartitions = 64,
        aggregationType = "count",
        groupingType = "customer_product"
      )
    )
  }

  // =========================================================================
  // MAIN
  // =========================================================================

  def main(args: Array[String]): Unit = {

    val config = parseArguments(args)

    printHeader(config)

    val spark =
      EnterpriseSparkSession.create(
        appName = "Module 1.8.3 - Shuffle Read Write Performance",
        environment = "dev"
      )

    val listener =
      new ShuffleMetricsListener()

    spark.sparkContext.addSparkListener(listener)

    var input: DataFrame = null

    try {

      // ---------------------------------------------------------------------
      // Primary experiment configuration.
      //
      // AQE is disabled initially.
      // Individual experiment cases can override this.
      // ---------------------------------------------------------------------

      spark.conf.set(
        "spark.sql.adaptive.enabled",
        "false"
      )

      // ---------------------------------------------------------------------
      // Create the fixed input dataset once.
      // ---------------------------------------------------------------------

      input =
        createInputDataset(
          spark = spark,
          rows = config.rows,
          distinctKeys = config.distinctKeys,
          inputPartitions = config.inputPartitions
        )

      println()
      println("INPUT DATASET")
      println("============================================================")
      println(s"Rows               : ${config.rows}")
      println(s"Distinct customers : ${config.distinctKeys}")
      println(s"Requested partitions: ${config.inputPartitions}")
      println(s"Actual partitions  : ${input.rdd.getNumPartitions}")

      // ---------------------------------------------------------------------
      // Validate the input partition count.
      // ---------------------------------------------------------------------

      require(
        input.rdd.getNumPartitions == config.inputPartitions,
        s"Input partition validation failed. " +
          s"Expected ${config.inputPartitions}, " +
          s"found ${input.rdd.getNumPartitions}."
      )

      println("Partition validation: PASSED")

      // ---------------------------------------------------------------------
      // Cache the input.
      //
      // Materialization happens before the experiment matrix.
      // ---------------------------------------------------------------------

      input.cache()

      listener.clear()

      val materializationStart =
        System.nanoTime()

      input.count()

      val materializationSeconds =
        (System.nanoTime() - materializationStart) /
          1000000000.0

      println(
        f"Input materialization time : $materializationSeconds%.3f seconds"
      )

      // ---------------------------------------------------------------------
      // Input schema.
      // ---------------------------------------------------------------------

      println()
      println("INPUT SCHEMA")
      println("============================================================")

      input.printSchema()

      // ---------------------------------------------------------------------
      // Primary experiment matrix.
      // ---------------------------------------------------------------------

      primaryExperimentMatrix.foreach { experiment =>

        listener.clear()

        runExperiment(
          spark = spark,
          input = input,
          experiment = experiment,
          listener = listener
        )
      }

      // ---------------------------------------------------------------------
      // AQE VALIDATION CASE
      // ---------------------------------------------------------------------

      listener.clear()

      runExperiment(
        spark = spark,
        input = input,
        experiment =
          ExperimentCase(
            name =
              "G — AQE VALIDATION — 64 CONFIGURED PARTITIONS",
            description =
              "Same two-key workload with AQE enabled to demonstrate " +
                "effective post-shuffle partition behavior.",
            shufflePartitions = 64,
            aggregationType = "count",
            groupingType = "customer_product",
            aqeEnabled = true
          ),
        listener = listener
      )

      printProductionGuidance()

      // ---------------------------------------------------------------------
      // Spark UI pause.
      // ---------------------------------------------------------------------

      if (config.uiPauseSeconds > 0) {

        println()
        println("============================================================")
        println("SPARK UI PAUSE")
        println("============================================================")

        println(
          s"Spark UI remains available for " +
            s"${config.uiPauseSeconds} seconds."
        )

        println("Open: http://localhost:4040")

        println()
        println("Recommended tabs:")
        println("  1. SQL")
        println("  2. Stages")
        println("  3. Stage Details")
        println("  4. Executors")

        println()
        println("Recommended evidence:")
        println("  - Exchange shuffle read/write")
        println("  - Number of tasks")
        println("  - Task duration distribution")
        println("  - Local vs remote shuffle read")
        println("  - Fetch wait")
        println("  - Shuffle blocks")
        println("  - Spill metrics")

        println("============================================================")

        Thread.sleep(
          config.uiPauseSeconds.toLong * 1000L
        )
      }

    } finally {

      // ---------------------------------------------------------------------
      // Deterministic input cleanup.
      // ---------------------------------------------------------------------

      if (input != null) {

        try {
          input.unpersist(blocking = false)
        } catch {
          case _: Throwable =>
            // Cleanup should never hide the primary experiment result.
        }
      }

      spark.stop()

      println()
      println("============================================================")
      println("MODULE 1.8.3 COMPLETED")
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

    /**
     * spark.range(..., numPartitions = inputPartitions)
     *
     * gives us an explicit and deterministic input partition count.
     *
     * This is important because the experiment is specifically designed to
     * compare shuffle behavior while holding the input partition count fixed.
     */
    spark
      .range(
        start = 0L,
        end = rows,
        step = 1L,
        numPartitions = inputPartitions
      )
      .select(
        $"id".as("transaction_id"),

        ($"id" % distinctKeys)
          .as("customer_id"),

        (($"id" * 31L) % 100000L)
          .as("product_id"),

        (($"id" * 17L) % 100000L)
          .cast("double")
          .as("amount")
      )
  }

  // =========================================================================
  // EXPERIMENT EXECUTION
  // =========================================================================

  private def runExperiment(
                             spark: SparkSession,
                             input: DataFrame,
                             experiment: ExperimentCase,
                             listener: ShuffleMetricsListener
                           ): Unit = {

    // -----------------------------------------------------------------------
    // Apply case-specific configuration.
    // -----------------------------------------------------------------------

    spark.conf.set(
      "spark.sql.shuffle.partitions",
      experiment.shufflePartitions
    )

    spark.conf.set(
      "spark.sql.adaptive.enabled",
      experiment.aqeEnabled.toString
    )

    println()
    println()
    println("################################################################")
    println(s"# EXPERIMENT: ${experiment.name}")
    println("################################################################")

    println()
    println("DESCRIPTION")
    println("------------------------------------------------------------")
    println(experiment.description)

    println()
    println("CONFIGURATION")
    println("------------------------------------------------------------")

    println(
      s"spark.sql.shuffle.partitions : " +
        s"${spark.conf.get("spark.sql.shuffle.partitions")}"
    )

    println(
      s"spark.sql.adaptive.enabled   : " +
        s"${spark.conf.get("spark.sql.adaptive.enabled")}"
    )

    // -------------------------------------------------------------------
    // FIX (SQL_CONF_NOT_FOUND):
    //
    // spark.shuffle.compress / spark.shuffle.spill.compress are CORE
    // Spark configs, not SQL configs. spark.conf.get(...) is backed by
    // SQLConf and can never resolve them, no matter when it's called.
    //
    // Read them from the SparkContext's SparkConf instead, with the
    // same defaults Spark itself uses ("true" for both).
    // -------------------------------------------------------------------

    println(
      s"spark.shuffle.compress       : " +
        s"${spark.sparkContext.getConf.get("spark.shuffle.compress", "true")}"
    )

    println(
      s"spark.shuffle.spill.compress : " +
        s"${spark.sparkContext.getConf.get("spark.shuffle.spill.compress", "true")}"
    )

    // -----------------------------------------------------------------------
    // Build workload.
    // -----------------------------------------------------------------------

    val result =
      buildWorkload(
        input = input,
        aggregationType = experiment.aggregationType,
        groupingType = experiment.groupingType
      )

    // -----------------------------------------------------------------------
    // Initial physical plan.
    //
    // This plan is inspected before the action.
    // -----------------------------------------------------------------------

    println()
    println("----- INITIAL PHYSICAL PLAN -----")
    println("------------------------------------------------------------")

    result.explain(mode = "formatted")

    // -----------------------------------------------------------------------
    // Clear any task metrics generated while constructing/materializing
    // intermediate state.
    // -----------------------------------------------------------------------

    listener.clear()

    val start =
      System.nanoTime()

    /**
     * count() forces the complete aggregation.
     *
     * We deliberately do not call show() first because doing so would create
     * another action and therefore complicate stage/task interpretation.
     */
    val resultCount =
      result.count()

    val elapsedSeconds =
      (System.nanoTime() - start) /
        1000000000.0

    // -----------------------------------------------------------------------
    // Basic result validation.
    // -----------------------------------------------------------------------

    require(
      resultCount > 0,
      s"Experiment ${experiment.name} returned zero rows."
    )

    println()
    println("RESULT")
    println("------------------------------------------------------------")

    println(
      s"Result rows : $resultCount"
    )

    println(
      f"Runtime     : $elapsedSeconds%.3f seconds"
    )

    // -----------------------------------------------------------------------
    // Executed physical plan.
    //
    // AQE changes become visible here.
    // -----------------------------------------------------------------------

    println()
    println("----- EXECUTED PHYSICAL PLAN -----")
    println("------------------------------------------------------------")

    println(
      result.queryExecution.executedPlan
    )

    val executedPlan =
      result.queryExecution.executedPlan.toString()

    // -----------------------------------------------------------------------
    // Operator validation.
    // -----------------------------------------------------------------------

    println()
    println("----- EXECUTED PLAN OPERATOR CHECK -----")
    println("------------------------------------------------------------")

    println(
      s"Exchange       : ${operatorStatus(executedPlan, "Exchange")}"
    )

    println(
      s"HashAggregate  : ${operatorStatus(executedPlan, "HashAggregate")}"
    )

    println(
      s"SortAggregate  : ${operatorStatus(executedPlan, "SortAggregate")}"
    )

    println(
      s"AQEShuffleRead : ${operatorStatus(executedPlan, "AQEShuffleRead")}"
    )

    // -----------------------------------------------------------------------
    // Task-level metrics.
    // -----------------------------------------------------------------------

    printShuffleMetrics(
      listener.snapshot()
    )

    // -----------------------------------------------------------------------
    // Spark UI guidance.
    // -----------------------------------------------------------------------

    println()
    println("SPARK UI ANALYSIS")
    println("------------------------------------------------------------")

    println(
      "SQL tab      → select this query → inspect Exchange metrics."
    )

    println(
      "Stages tab   → inspect shuffle read/write and task distribution."
    )

    println(
      "Stage Details → inspect individual task shuffle metrics."
    )

    println(
      "Executors    → inspect executor execution and spill information."
    )
  }

  // =========================================================================
  // WORKLOAD CONSTRUCTION
  // =========================================================================

  private def buildWorkload(
                             input: DataFrame,
                             aggregationType: String,
                             groupingType: String
                           ): DataFrame = {

    import org.apache.spark.sql.functions._

    val grouped =
      groupingType match {

        case "customer_id" =>

          input.groupBy("customer_id")

        case "customer_product" =>

          input.groupBy(
            "customer_id",
            "product_id"
          )

        case other =>

          throw new IllegalArgumentException(
            s"Unsupported grouping type: $other"
          )
      }

    aggregationType match {

      case "count" =>

        grouped.count()

      case "count_sum" =>

        grouped.agg(
          count(lit(1))
            .as("transaction_count"),

          sum("amount")
            .as("total_amount")
        )

      case other =>

        throw new IllegalArgumentException(
          s"Unsupported aggregation type: $other"
        )
    }
  }

  // =========================================================================
  // SHUFFLE METRICS REPORTING
  // =========================================================================

  private def printShuffleMetrics(
                                   metrics: Seq[TaskShuffleMetrics]
                                 ): Unit = {

    println()
    println("----- SHUFFLE I/O METRICS -----")
    println("------------------------------------------------------------")

    if (metrics.isEmpty) {

      println(
        "No task-level shuffle metrics were captured."
      )

      return
    }

    // -----------------------------------------------------------------------
    // Aggregate metrics.
    // -----------------------------------------------------------------------

    val shuffleReadBytes =
      metrics.map(_.shuffleReadBytes).sum

    val localReadBytes =
      metrics.map(_.localReadBytes).sum

    val remoteReadBytes =
      metrics.map(_.remoteReadBytes).sum

    val remoteReadToDiskBytes =
      metrics.map(_.remoteReadToDiskBytes).sum

    val shuffleReadRecords =
      metrics.map(_.shuffleReadRecords).sum

    val localBlocks =
      metrics.map(_.localBlocks).sum

    val remoteBlocks =
      metrics.map(_.remoteBlocks).sum

    val fetchWaitMs =
      metrics.map(_.fetchWaitTimeMs).sum

    val shuffleWriteBytes =
      metrics.map(_.shuffleWriteBytes).sum

    val shuffleWriteRecords =
      metrics.map(_.shuffleWriteRecords).sum

    val shuffleWriteTimeMs =
      metrics.map(_.shuffleWriteTimeMs).sum

    val memoryBytesSpilled =
      metrics.map(_.memoryBytesSpilled).sum

    val diskBytesSpilled =
      metrics.map(_.diskBytesSpilled).sum

    // -----------------------------------------------------------------------
    // Print aggregate values.
    // -----------------------------------------------------------------------

    println(
      s"Shuffle read bytes       : ${formatBytes(shuffleReadBytes)}"
    )

    println(
      s"Local shuffle read      : ${formatBytes(localReadBytes)}"
    )

    println(
      s"Remote shuffle read     : ${formatBytes(remoteReadBytes)}"
    )

    println(
      s"Remote read to disk     : ${formatBytes(remoteReadToDiskBytes)}"
    )

    println(
      f"Shuffle read records    : ${shuffleReadRecords}%,d"
    )

    println(
      f"Local shuffle blocks    : ${localBlocks}%,d"
    )

    println(
      f"Remote shuffle blocks   : ${remoteBlocks}%,d"
    )

    println(
      f"Fetch wait time         : ${fetchWaitMs}%,d ms"
    )

    println(
      s"Shuffle write bytes     : ${formatBytes(shuffleWriteBytes)}"
    )

    println(
      f"Shuffle write records   : ${shuffleWriteRecords}%,d"
    )

    println(
      f"Shuffle write time      : ${shuffleWriteTimeMs}%,d ms"
    )

    println(
      s"Memory bytes spilled     : ${formatBytes(memoryBytesSpilled)}"
    )

    println(
      s"Disk bytes spilled       : ${formatBytes(diskBytesSpilled)}"
    )

    // -----------------------------------------------------------------------
    // Read locality percentages.
    // -----------------------------------------------------------------------

    if (shuffleReadBytes > 0) {

      val localPct =
        localReadBytes.toDouble /
          shuffleReadBytes.toDouble *
          100.0

      val remotePct =
        remoteReadBytes.toDouble /
          shuffleReadBytes.toDouble *
          100.0

      println()
      println("----- SHUFFLE READ LOCALITY -----")
      println("------------------------------------------------------------")

      println(
        f"Local read percentage  : $localPct%.2f%%"
      )

      println(
        f"Remote read percentage : $remotePct%.2f%%"
      )
    }

    // -----------------------------------------------------------------------
    // Task-level variation.
    // -----------------------------------------------------------------------

    val tasksWithShuffle =
      metrics.filter { metric =>
        metric.shuffleReadBytes > 0 ||
          metric.shuffleWriteBytes > 0
      }

    println()
    println("----- TASK-LEVEL VARIATION -----")
    println("------------------------------------------------------------")

    println(
      s"Tasks with shuffle activity : ${tasksWithShuffle.size}"
    )

    if (tasksWithShuffle.nonEmpty) {

      val readValues =
        tasksWithShuffle.map(_.shuffleReadBytes)

      val writeValues =
        tasksWithShuffle.map(_.shuffleWriteBytes)

      val fetchValues =
        tasksWithShuffle.map(_.fetchWaitTimeMs)

      val readRecords =
        tasksWithShuffle.map(_.shuffleReadRecords)

      println(
        s"Shuffle read min/max : " +
          s"${formatBytes(readValues.min)} / " +
          s"${formatBytes(readValues.max)}"
      )

      println(
        s"Shuffle write min/max: " +
          s"${formatBytes(writeValues.min)} / " +
          s"${formatBytes(writeValues.max)}"
      )

      println(
        s"Fetch wait min/max   : " +
          f"${fetchValues.min}%,d / ${fetchValues.max}%,d ms"
      )

      println(
        s"Read records min/max : " +
          f"${readRecords.min}%,d / ${readRecords.max}%,d"
      )

      // ---------------------------------------------------------------------
      // Simple imbalance indicators.
      //
      // These are descriptive metrics, not statistical skew tests.
      // ---------------------------------------------------------------------

      val readAverage =
        readValues.sum.toDouble /
          readValues.size

      val writeAverage =
        writeValues.sum.toDouble /
          writeValues.size

      println()
      println("----- SIMPLE TASK IMBALANCE INDICATORS -----")
      println("------------------------------------------------------------")

      println(
        f"Average shuffle read  : ${formatBytes(readAverage.toLong)}"
      )

      println(
        f"Average shuffle write : ${formatBytes(writeAverage.toLong)}"
      )

      if (readAverage > 0) {

        val maxToAverageRead =
          readValues.max.toDouble /
            readAverage

        println(
          f"Max read / average read   : $maxToAverageRead%.2fx"
        )
      }

      if (writeAverage > 0) {

        val maxToAverageWrite =
          writeValues.max.toDouble /
            writeAverage

        println(
          f"Max write / average write : $maxToAverageWrite%.2fx"
        )
      }

      // ---------------------------------------------------------------------
      // Detailed task output.
      // ---------------------------------------------------------------------

      println()
      println(
        "Task metrics — stage/task/partition/read/write/fetch:"
      )

      tasksWithShuffle
        .sortBy(metric => (metric.stageId, metric.partitionId))
        .foreach { metric =>

          println(
            "  " +
              s"stage=${metric.stageId} " +
              s"task=${metric.taskId} " +
              s"partition=${metric.partitionId} " +
              s"read=${formatBytes(metric.shuffleReadBytes)} " +
              s"local=${formatBytes(metric.localReadBytes)} " +
              s"remote=${formatBytes(metric.remoteReadBytes)} " +
              s"write=${formatBytes(metric.shuffleWriteBytes)} " +
              s"fetchWait=${metric.fetchWaitTimeMs} ms " +
              s"spill=${formatBytes(metric.diskBytesSpilled)}"
          )
        }
    }
  }

  // =========================================================================
  // OPERATOR VALIDATION
  // =========================================================================

  private def operatorStatus(
                              plan: String,
                              operator: String
                            ): String = {

    if (plan.contains(operator))
      "PRESENT"
    else
      "not present"
  }

  // =========================================================================
  // BYTE FORMATTING
  // =========================================================================

  private def formatBytes(
                           bytes: Long
                         ): String = {

    if (bytes < 1024L) {

      s"$bytes B"

    } else if (bytes < 1024L * 1024L) {

      f"${bytes / 1024.0}%.2f KiB"

    } else if (bytes < 1024L * 1024L * 1024L) {

      f"${bytes / (1024.0 * 1024.0)}%.2f MiB"

    } else {

      f"${bytes / (1024.0 * 1024.0 * 1024.0)}%.2f GiB"
    }
  }

  // =========================================================================
  // ARGUMENT CONFIGURATION
  // =========================================================================

  final case class ExperimentConfig(
                                     rows: Long,
                                     distinctKeys: Long,
                                     inputPartitions: Int,
                                     shufflePartitions: Int = 24,
                                     uiPauseSeconds: Int = 0
                                   )

  // =========================================================================
  // ARGUMENT PARSING
  // =========================================================================

  private def parseArguments(
                              args: Array[String]
                            ): ExperimentConfig = {

    val positional =
      args.filterNot(
        _.startsWith("--")
      )

    val rows =
      positional
        .headOption
        .map(_.toLong)
        .getOrElse(DefaultRows)

    val distinctKeys =
      positional
        .drop(1)
        .headOption
        .map(_.toLong)
        .getOrElse(DefaultDistinctKeys)

    val inputPartitions =
      positional
        .drop(2)
        .headOption
        .map(_.toInt)
        .getOrElse(DefaultInputPartitions)

    val uiPauseSeconds =
      args
        .find(
          _.startsWith("--ui-pause=")
        )
        .map(
          _.stripPrefix("--ui-pause=").toInt
        )
        .getOrElse(0)

    validateArguments(
      rows = rows,
      distinctKeys = distinctKeys,
      inputPartitions = inputPartitions,
      uiPauseSeconds = uiPauseSeconds
    )

    ExperimentConfig(
      rows = rows,
      distinctKeys = distinctKeys,
      inputPartitions = inputPartitions,
      uiPauseSeconds = uiPauseSeconds
    )
  }

  // =========================================================================
  // ARGUMENT VALIDATION
  // =========================================================================

  private def validateArguments(
                                 rows: Long,
                                 distinctKeys: Long,
                                 inputPartitions: Int,
                                 uiPauseSeconds: Int
                               ): Unit = {

    require(
      rows > 0,
      "rows must be greater than zero."
    )

    require(
      distinctKeys > 0 &&
        distinctKeys <= rows,
      "distinctKeys must be > 0 and <= rows."
    )

    require(
      inputPartitions > 0,
      "inputPartitions must be greater than zero."
    )

    require(
      uiPauseSeconds >= 0,
      "ui-pause must be zero or greater."
    )
  }

  // =========================================================================
  // HEADER
  // =========================================================================

  private def printHeader(
                           config: ExperimentConfig
                         ): Unit = {

    println()
    println("============================================================")
    println("MODULE 1.8.3 — SHUFFLE READ / WRITE PERFORMANCE")
    println("============================================================")

    println(
      s"Input rows       : ${config.rows}"
    )

    println(
      s"Distinct keys    : ${config.distinctKeys}"
    )

    println(
      s"Input partitions : ${config.inputPartitions}"
    )

    println(
      s"UI pause         : ${config.uiPauseSeconds} seconds"
    )

    println("============================================================")
  }

  // =========================================================================
  // PRODUCTION ENGINEERING GUIDANCE
  // =========================================================================

  private def printProductionGuidance(): Unit = {

    println()
    println("============================================================")
    println("PRODUCTION ENGINEERING QUESTIONS")
    println("============================================================")

    println(
      """
1. How much data is written into the shuffle?

2. How much data is actually read from the shuffle?

3. What percentage of shuffle reads are local versus remote?

4. Is remote shuffle data being read from disk?

5. How many local and remote shuffle blocks are fetched?

6. Is fetch wait time significant?

7. Are shuffle writes evenly distributed across map tasks?

8. Are shuffle reads evenly distributed across reduce tasks?

9. Does increasing shuffle partitions reduce per-task I/O?

10. Does increasing shuffle partitions create many small shuffle blocks?

11. Does the physical plan contain the expected Exchange boundary?

12. Does AQE change the effective post-shuffle partitioning?

13. Does the AQE case contain AQEShuffleRead?

14. Does runtime improvement correspond to lower shuffle I/O?

15. Is the improvement instead caused primarily by greater task parallelism?

16. Are spill metrics significant?

17. Is one task responsible for a disproportionate amount of shuffle data?

18. Does local versus remote shuffle behavior explain fetch wait time?

The goal is to correlate:

    physical plan
          ↓
       Exchange
          ↓
        stage
          ↓
         task
          ↓
    shuffle metrics
          ↓
       runtime

rather than optimizing from runtime alone.
"""
    )
  }
}