package com.shrikant.spark.shuffle

import org.apache.spark.scheduler.{SparkListener, SparkListenerTaskEnd}
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable.ArrayBuffer

/**
 * Module 1.8.4
 *
 * Shuffle Spill & Memory
 *
 * ===========================================================================
 * PURPOSE
 * ===========================================================================
 *
 * Module 1.8.3 measured shuffle I/O under AMPLE execution memory and
 * observed ZERO spill (memory or disk) across every one of its seven
 * experiment cases, at 10,000,000 rows / 1,000,000 distinct keys.
 *
 * This module exists specifically to produce the opposite condition: real,
 * measurable spill. The dataset from 1.8.3 is kept for continuity, but the
 * available execution memory is deliberately constrained so that
 * aggregation buffers are forced to overflow.
 *
 * The question this module answers:
 *
 *   What actually happens to shuffle/aggregation performance when working
 *   data no longer fits in execution memory, and what conditions make
 *   spill worse or better?
 *
 * ===========================================================================
 * CRITICAL DESIGN CONSTRAINT — WHY THIS CLASS RUNS ONE CASE PER JVM
 * ===========================================================================
 *
 * Module 1.8.3 looped its entire experiment matrix inside a single
 * SparkSession/JVM because the variable under test
 * (spark.sql.shuffle.partitions, spark.sql.adaptive.enabled) is a SQL
 * runtime config that CAN be mutated between queries via spark.conf.set(...).
 *
 * This module's variable — available execution memory — is governed by
 * spark.memory.fraction, which UnifiedMemoryManager reads ONCE when the
 * SparkContext is constructed. It cannot be changed after that, for the
 * same underlying reason spark.shuffle.compress could not be read via
 * spark.conf.get in 1.8.3: core memory-management configuration is fixed
 * at Spark/JVM initialization, not re-readable or re-settable mid-session.
 *
 * Consequently, EACH memory-pressure case in this module's matrix requires
 * its OWN process invocation. This class takes a single --case=<LETTER>
 * argument and runs exactly one configuration per run. The six cases (A–F)
 * from the design are run as six separate Gradle invocations; evidence
 * from each run is collected independently and compared afterward.
 *
 * This is also why this class builds its own SparkSession directly rather
 * than going through EnterpriseSparkSession.create(...): spark.memory.fraction
 * must be set on the SparkConf BEFORE the SparkSession/SparkContext is
 * built, and this module needs that value to vary per invocation in a way
 * EnterpriseSparkSession's current signature does not expose.
 *
 * ===========================================================================
 * EXPERIMENT MATRIX
 * ===========================================================================
 *
 * Fixed across all cases: 10,000,000 rows, 1,000,000 distinct keys,
 * 6 input partitions, same input-generation logic as Module 1.8.3.
 *
 *   A — BASELINE_AMPLE_MEMORY
 *       spark.memory.fraction = default (0.6), 24 shuffle partitions.
 *       Reconfirms 1.8.3's "no spill" result as a control case.
 *
 *   B — CONSTRAINED_MEMORY
 *       spark.memory.fraction reduced sharply, 24 shuffle partitions,
 *       same single-key aggregation as 1.8.3's Experiment B.
 *       Expected to force spill with data volume held fixed.
 *
 *   C — CONSTRAINED_MEMORY_WIDE_AGG
 *       Same memory constraint as B, two-key grouping
 *       (customer_id, product_id) — larger per-key aggregation buffer.
 *       Tests whether more distinct groups spill sooner under the same
 *       memory cap.
 *
 *   D — CONSTRAINED_MEMORY_FEW_PARTITIONS
 *       Same memory constraint as B, 6 shuffle partitions.
 *       Larger per-task partitions under pressure — expected to spill
 *       MORE than B.
 *
 *   E — CONSTRAINED_MEMORY_MANY_PARTITIONS
 *       Same memory constraint as B, 64 shuffle partitions.
 *       Smaller per-task partitions under the same pressure — expected
 *       to spill LESS than B, possibly not at all.
 *
 *   F — CONSTRAINED_MEMORY_AQE
 *       Same memory constraint as B, 64 configured shuffle partitions,
 *       AQE enabled, two-key grouping. Tests whether AQE's post-shuffle
 *       coalescing (which concentrates data onto fewer tasks, as seen in
 *       1.8.3's Experiment G) makes spill WORSE under memory pressure.
 *
 * ===========================================================================
 * EXECUTION
 * ===========================================================================
 *
 * ./gradlew runShuffleSpillMemoryExperiment --args="--case=A"
 * ./gradlew runShuffleSpillMemoryExperiment --args="--case=B --ui-pause=120"
 * ./gradlew runShuffleSpillMemoryExperiment --args="--case=C"
 * ./gradlew runShuffleSpillMemoryExperiment --args="--case=D"
 * ./gradlew runShuffleSpillMemoryExperiment --args="--case=E"
 * ./gradlew runShuffleSpillMemoryExperiment --args="--case=F"
 *
 * Optional positional overrides (rows, distinctKeys, inputPartitions) are
 * supported for follow-up investigation but default to the 1.8.3 dataset:
 *
 * ./gradlew runShuffleSpillMemoryExperiment \
 *   --args="--case=B 10000000 1000000 6 --ui-pause=120"
 *
 * ===========================================================================
 * IMPORTANT CAVEAT — local[2] MEMORY CEILING
 * ===========================================================================
 *
 * spark.memory.fraction constrains the FRACTION of (JVM heap - 300 MiB
 * reserved) available to execution + storage. It does not itself set an
 * absolute ceiling. The absolute memory available is still bounded by the
 * JVM heap size this process is launched with (-Xmx, configured via Gradle
 * JavaExec / --add-opens flags already in use for this repo). If the
 * configured memory.fraction value does not produce spill at the fixed
 * dataset size, the next lever is lowering -Xmx itself (a build.gradle.kts
 * change), not this class — that is intentionally left as a follow-up
 * rather than hard-coded here, since the correct value depends on the
 * actual heap size this JVM is launched with on this machine.
 *
 * ===========================================================================
 */
object ShuffleSpillMemoryExperiment {

  // =========================================================================
  // DEFAULT CONFIGURATION
  // =========================================================================

  private val DefaultRows = 10000000L
  private val DefaultDistinctKeys = 1000000L
  private val DefaultInputPartitions = 6

  /** Spark's own default. Used for the "ample memory" control case. */
  private val AmpleMemoryFraction = 0.6

  /**
   * Deliberately aggressive reduction for the constrained-memory cases.
   * Spark's minimum enforced value is low, but values this small leave
   * very little room for aggregation hash maps before spilling.
   * If initial runs show this still doesn't spill at this dataset size,
   * reduce further via --memory-fraction=<value> (see parseArguments).
   */
  private val ConstrainedMemoryFraction = 0.1

  // =========================================================================
  // TASK-LEVEL METRICS
  // =========================================================================

  /**
   * Captures shuffle AND spill metrics for one completed Spark task.
   *
   * memoryBytesSpilled / diskBytesSpilled were already part of 1.8.3's
   * listener but were never exercised there (always 0). This module is
   * where they become the primary metric of interest.
   */
  private final case class TaskSpillMetrics(
                                             stageId: Int,
                                             taskId: Long,
                                             partitionId: Int,

                                             shuffleReadBytes: Long,
                                             shuffleWriteBytes: Long,

                                             memoryBytesSpilled: Long,
                                             diskBytesSpilled: Long,

                                             executorRunTimeMs: Long
                                           )

  // =========================================================================
  // SPARK LISTENER
  // =========================================================================

  private final class SpillMetricsListener extends SparkListener {

    private val metricsBuffer =
      ArrayBuffer.empty[TaskSpillMetrics]

    override def onTaskEnd(
                            taskEnd: SparkListenerTaskEnd
                          ): Unit = synchronized {

      val metrics = taskEnd.taskMetrics

      if (metrics != null) {

        val read = metrics.shuffleReadMetrics
        val write = metrics.shuffleWriteMetrics

        metricsBuffer += TaskSpillMetrics(

          stageId = taskEnd.stageId,

          taskId =
            if (taskEnd.taskInfo != null) taskEnd.taskInfo.taskId else -1L,

          partitionId =
            if (taskEnd.taskInfo != null) taskEnd.taskInfo.index else -1,

          shuffleReadBytes =
            if (read != null) read.totalBytesRead else 0L,

          shuffleWriteBytes =
            if (write != null) write.bytesWritten else 0L,

          memoryBytesSpilled = metrics.memoryBytesSpilled,
          diskBytesSpilled = metrics.diskBytesSpilled,

          executorRunTimeMs = metrics.executorRunTime
        )
      }
    }

    def snapshot(): Seq[TaskSpillMetrics] =
      synchronized { metricsBuffer.toVector }

    def clear(): Unit =
      synchronized { metricsBuffer.clear() }
  }

  // =========================================================================
  // CASE DEFINITION
  // =========================================================================

  private final case class SpillCase(
                                      label: String,
                                      description: String,
                                      memoryFraction: Double,
                                      shufflePartitions: Int,
                                      groupingType: String,
                                      aqeEnabled: Boolean
                                    )

  private val CaseMatrix: Map[String, SpillCase] = Map(

    "A" -> SpillCase(
      label = "A — BASELINE, AMPLE MEMORY",
      description =
        "Control case: default memory.fraction, should reproduce 1.8.3's " +
          "zero-spill result at this dataset size.",
      memoryFraction = AmpleMemoryFraction,
      shufflePartitions = 24,
      groupingType = "customer_id",
      aqeEnabled = false
    ),

    "B" -> SpillCase(
      label = "B — CONSTRAINED MEMORY",
      description =
        "Memory fraction sharply reduced, single-key aggregation, same " +
          "partition count as 1.8.3's Experiment B. Primary spill case.",
      memoryFraction = ConstrainedMemoryFraction,
      shufflePartitions = 24,
      groupingType = "customer_id",
      aqeEnabled = false
    ),

    "C" -> SpillCase(
      label = "C — CONSTRAINED MEMORY, WIDE AGGREGATION",
      description =
        "Same memory cap as B, two-key grouping — larger aggregation " +
          "buffer per task.",
      memoryFraction = ConstrainedMemoryFraction,
      shufflePartitions = 24,
      groupingType = "customer_product",
      aqeEnabled = false
    ),

    "D" -> SpillCase(
      label = "D — CONSTRAINED MEMORY, FEW PARTITIONS",
      description =
        "Same memory cap as B, 6 shuffle partitions — larger per-task " +
          "shuffle input under the same pressure.",
      memoryFraction = ConstrainedMemoryFraction,
      shufflePartitions = 6,
      groupingType = "customer_id",
      aqeEnabled = false
    ),

    "E" -> SpillCase(
      label = "E — CONSTRAINED MEMORY, MANY PARTITIONS",
      description =
        "Same memory cap as B, 64 shuffle partitions — smaller per-task " +
          "shuffle input under the same pressure.",
      memoryFraction = ConstrainedMemoryFraction,
      shufflePartitions = 64,
      groupingType = "customer_id",
      aqeEnabled = false
    ),

    "F" -> SpillCase(
      label = "F — CONSTRAINED MEMORY, AQE ENABLED",
      description =
        "Same memory cap as B, 64 configured shuffle partitions, AQE on, " +
          "two-key grouping. Tests whether AQE coalescing concentrates " +
          "spill onto fewer tasks.",
      memoryFraction = ConstrainedMemoryFraction,
      shufflePartitions = 64,
      groupingType = "customer_product",
      aqeEnabled = true
    )
  )

  // =========================================================================
  // MAIN
  // =========================================================================

  def main(args: Array[String]): Unit = {

    val config = parseArguments(args)
    val baseCase = CaseMatrix(config.caseLabel)

    // Apply an explicit --memory-fraction override, if provided, on top of
    // the case's default — lets you probe for the spill threshold without
    // editing the matrix.
    val spillCase =
      config.memoryFractionOverride match {
        case Some(f) => baseCase.copy(memoryFraction = f)
        case None => baseCase
      }

    printHeader(config, spillCase)

    // -------------------------------------------------------------------
    // Build the SparkSession DIRECTLY (not via EnterpriseSparkSession).
    //
    // spark.memory.fraction must be present on the SparkConf before the
    // SparkContext is constructed. It is read once by UnifiedMemoryManager
    // and is not mutable afterward via spark.conf.set(...) — the same
    // category of constraint that caused the 1.8.3 SQL_CONF_NOT_FOUND
    // failure, just for a config that genuinely must be set pre-init
    // rather than one that was simply queried through the wrong API.
    // -------------------------------------------------------------------

    val spark =
      SparkSession.builder()
        .appName(
          s"Module 1.8.4 - Shuffle Spill and Memory - Case ${config.caseLabel}"
        )
        .master("local[2]")
        .config("spark.memory.fraction", spillCase.memoryFraction)
        .config("spark.sql.shuffle.partitions", spillCase.shufflePartitions)
        .config("spark.sql.adaptive.enabled", spillCase.aqeEnabled)
        .getOrCreate()

    val listener = new SpillMetricsListener()
    spark.sparkContext.addSparkListener(listener)

    var input: DataFrame = null

    try {

      // ---------------------------------------------------------------------
      // Confirm the memory configuration actually took effect.
      //
      // Read back from sparkContext.getConf (core SparkConf), not
      // spark.conf.get — spark.memory.fraction is a core config, same
      // category as spark.shuffle.compress in 1.8.3.
      // ---------------------------------------------------------------------

      val effectiveMemoryFraction =
        spark.sparkContext.getConf.get("spark.memory.fraction", "unset")

      println(s"Effective spark.memory.fraction : $effectiveMemoryFraction")
      println(s"JVM max heap (bytes)             : ${Runtime.getRuntime.maxMemory()}")

      // ---------------------------------------------------------------------
      // Create and materialize the fixed input dataset (same logic as
      // Module 1.8.3, for direct comparability).
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
      println(s"Input partitions   : ${input.rdd.getNumPartitions}")

      // -------------------------------------------------------------------
      // DELIBERATELY NOT CACHED (unlike 1.8.3).
      //
      // 1.8.3 cached the input because it reused the SAME DataFrame across
      // seven experiment cases in one JVM. This module runs exactly one
      // case per JVM (see class-level comment), so caching buys no reuse
      // benefit here.
      //
      // Worse, under the memory pressure this module deliberately creates,
      // caching is actively harmful: cached blocks are PINNED in the
      // unified memory pool while in use, competing directly with the
      // aggregation's execution memory for the same limited pool. In an
      // earlier run at this configuration, caching caused storage to claim
      // nearly the entire pool, leaving execution with no room even to
      // initialize its spill-to-disk fallback, producing a hard
      // SparkOutOfMemoryError instead of the graceful, measurable spill
      // this module exists to observe.
      //
      // Leaving the input uncached means it is recomputed from the cheap
      // deterministic spark.range(...) transform on each access — that
      // recomputation cost is irrelevant here, since this module measures
      // AGGREGATION-side spill, not input-read performance.
      // -------------------------------------------------------------------

      listener.clear()

      // ---------------------------------------------------------------------
      // Run the single configured case.
      // ---------------------------------------------------------------------

      runCase(
        spark = spark,
        input = input,
        spillCase = spillCase,
        listener = listener
      )

      // ---------------------------------------------------------------------
      // Spark UI pause.
      // ---------------------------------------------------------------------

      if (config.uiPauseSeconds > 0) {

        println()
        println("============================================================")
        println("SPARK UI PAUSE")
        println("============================================================")
        println(s"Spark UI remains available for ${config.uiPauseSeconds} seconds.")
        println("Open: http://localhost:4040")
        println()
        println("Recommended evidence:")
        println("  - Stages tab: spill (memory) / spill (disk) columns")
        println("  - Stage Details: per-task spill distribution")
        println("  - Executors tab: peak execution memory vs. configured fraction")
        println("============================================================")

        Thread.sleep(config.uiPauseSeconds.toLong * 1000L)
      }

    } finally {

      if (input != null) {
        try {
          input.unpersist(blocking = false)
        } catch {
          case _: Throwable => // cleanup must never hide the primary result
        }
      }

      spark.stop()

      println()
      println("============================================================")
      println(s"MODULE 1.8.4 CASE ${config.caseLabel} COMPLETED")
      println("============================================================")
    }
  }

  // =========================================================================
  // INPUT DATASET — identical generation logic to Module 1.8.3
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
                       spillCase: SpillCase,
                       listener: SpillMetricsListener
                     ): Unit = {

    import org.apache.spark.sql.functions._

    println()
    println("################################################################")
    println(s"# EXPERIMENT: ${spillCase.label}")
    println("################################################################")
    println()
    println("DESCRIPTION")
    println("------------------------------------------------------------")
    println(spillCase.description)

    println()
    println("CONFIGURATION")
    println("------------------------------------------------------------")
    println(s"spark.memory.fraction         : ${spillCase.memoryFraction}")
    println(s"spark.sql.shuffle.partitions  : ${spark.conf.get("spark.sql.shuffle.partitions")}")
    println(s"spark.sql.adaptive.enabled    : ${spark.conf.get("spark.sql.adaptive.enabled")}")
    println(s"Grouping                      : ${spillCase.groupingType}")

    val grouped =
      spillCase.groupingType match {
        case "customer_id" => input.groupBy("customer_id")
        case "customer_product" => input.groupBy("customer_id", "product_id")
        case other => throw new IllegalArgumentException(s"Unsupported grouping type: $other")
      }

    val result = grouped.agg(
      count(lit(1)).as("transaction_count"),
      sum("amount").as("total_amount"),
      avg("amount").as("avg_amount")
    )

    println()
    println("----- INITIAL PHYSICAL PLAN -----")
    println("------------------------------------------------------------")
    result.explain(mode = "formatted")

    listener.clear()
    val start = System.nanoTime()
    val resultCount = result.count()
    val elapsedSeconds = (System.nanoTime() - start) / 1000000000.0

    require(resultCount > 0, s"Experiment ${spillCase.label} returned zero rows.")

    println()
    println("RESULT")
    println("------------------------------------------------------------")
    println(s"Result rows : $resultCount")
    println(f"Runtime     : $elapsedSeconds%.3f seconds")

    println()
    println("----- EXECUTED PHYSICAL PLAN -----")
    println("------------------------------------------------------------")
    println(result.queryExecution.executedPlan)

    printSpillMetrics(listener.snapshot())
  }

  // =========================================================================
  // SPILL METRICS REPORTING
  // =========================================================================

  private def printSpillMetrics(metrics: Seq[TaskSpillMetrics]): Unit = {

    println()
    println("----- SPILL METRICS -----")
    println("------------------------------------------------------------")

    if (metrics.isEmpty) {
      println("No task metrics were captured.")
      return
    }

    val totalMemorySpilled = metrics.map(_.memoryBytesSpilled).sum
    val totalDiskSpilled = metrics.map(_.diskBytesSpilled).sum
    val totalShuffleRead = metrics.map(_.shuffleReadBytes).sum
    val totalShuffleWrite = metrics.map(_.shuffleWriteBytes).sum
    val totalRunTimeMs = metrics.map(_.executorRunTimeMs).sum

    println(s"Memory bytes spilled (total) : ${formatBytes(totalMemorySpilled)}")
    println(s"Disk bytes spilled (total)   : ${formatBytes(totalDiskSpilled)}")
    println(s"Shuffle read (total)         : ${formatBytes(totalShuffleRead)}")
    println(s"Shuffle write (total)        : ${formatBytes(totalShuffleWrite)}")
    println(s"Sum of task executor runtime : ${totalRunTimeMs} ms")

    if (totalShuffleWrite > 0) {
      val spillRatio = totalDiskSpilled.toDouble / totalShuffleWrite.toDouble * 100.0
      println(f"Disk spill as %% of shuffle write : $spillRatio%.2f%%")
    }

    val spilledTasks = metrics.filter(m => m.memoryBytesSpilled > 0 || m.diskBytesSpilled > 0)

    println()
    println("----- TASK-LEVEL SPILL VARIATION -----")
    println("------------------------------------------------------------")
    println(s"Tasks with spill activity : ${spilledTasks.size} / ${metrics.size}")

    if (spilledTasks.nonEmpty) {

      val memValues = spilledTasks.map(_.memoryBytesSpilled)
      val diskValues = spilledTasks.map(_.diskBytesSpilled)

      println(s"Memory spill min/max : ${formatBytes(memValues.min)} / ${formatBytes(memValues.max)}")
      println(s"Disk spill min/max   : ${formatBytes(diskValues.min)} / ${formatBytes(diskValues.max)}")

      println()
      println("Task spill detail — stage/task/partition/memorySpill/diskSpill/runtime:")

      spilledTasks
        .sortBy(m => (m.stageId, m.partitionId))
        .foreach { m =>
          println(
            "  " +
              s"stage=${m.stageId} task=${m.taskId} partition=${m.partitionId} " +
              s"memSpill=${formatBytes(m.memoryBytesSpilled)} " +
              s"diskSpill=${formatBytes(m.diskBytesSpilled)} " +
              s"runtime=${m.executorRunTimeMs} ms"
          )
        }
    } else {
      println("No spill observed in this case — consider lowering spark.memory.fraction")
      println("further (see --memory-fraction override) or reducing the JVM's -Xmx.")
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
  // ARGUMENT CONFIGURATION
  // =========================================================================

  final case class ExperimentConfig(
                                     caseLabel: String,
                                     rows: Long,
                                     distinctKeys: Long,
                                     inputPartitions: Int,
                                     uiPauseSeconds: Int,
                                     memoryFractionOverride: Option[Double]
                                   )

  // =========================================================================
  // ARGUMENT PARSING
  // =========================================================================

  private def parseArguments(args: Array[String]): ExperimentConfig = {

    val caseLabel =
      args
        .find(_.startsWith("--case="))
        .map(_.stripPrefix("--case=").toUpperCase)
        .getOrElse(
          throw new IllegalArgumentException(
            "Missing required --case=<A|B|C|D|E|F> argument. " +
              "Each memory-pressure case must be run as its own process " +
              "(see class-level comment for why)."
          )
        )

    require(
      CaseMatrix.contains(caseLabel),
      s"Unknown case '$caseLabel'. Valid cases: ${CaseMatrix.keys.toSeq.sorted.mkString(", ")}"
    )

    val memoryFractionOverride =
      args
        .find(_.startsWith("--memory-fraction="))
        .map(_.stripPrefix("--memory-fraction=").toDouble)

    val positional = args.filterNot(_.startsWith("--"))

    val rows = positional.headOption.map(_.toLong).getOrElse(DefaultRows)
    val distinctKeys = positional.drop(1).headOption.map(_.toLong).getOrElse(DefaultDistinctKeys)
    val inputPartitions = positional.drop(2).headOption.map(_.toInt).getOrElse(DefaultInputPartitions)

    val uiPauseSeconds =
      args
        .find(_.startsWith("--ui-pause="))
        .map(_.stripPrefix("--ui-pause=").toInt)
        .getOrElse(0)

    require(rows > 0, "rows must be greater than zero.")
    require(distinctKeys > 0 && distinctKeys <= rows, "distinctKeys must be > 0 and <= rows.")
    require(inputPartitions > 0, "inputPartitions must be greater than zero.")
    require(uiPauseSeconds >= 0, "ui-pause must be zero or greater.")
    memoryFractionOverride.foreach { f =>
      require(f > 0.0 && f < 1.0, "memory-fraction override must be between 0 and 1 exclusive.")
    }

    ExperimentConfig(
      caseLabel = caseLabel,
      rows = rows,
      distinctKeys = distinctKeys,
      inputPartitions = inputPartitions,
      uiPauseSeconds = uiPauseSeconds,
      memoryFractionOverride = memoryFractionOverride
    )
  }

  // =========================================================================
  // HEADER
  // =========================================================================

  private def printHeader(config: ExperimentConfig, spillCase: SpillCase): Unit = {
    println()
    println("============================================================")
    println("MODULE 1.8.4 — SHUFFLE SPILL & MEMORY")
    println("============================================================")
    println(s"Case              : ${config.caseLabel} — ${spillCase.label}")
    println(s"Rows              : ${config.rows}")
    println(s"Distinct keys     : ${config.distinctKeys}")
    println(s"Input partitions  : ${config.inputPartitions}")
    println(s"Shuffle partitions: ${spillCase.shufflePartitions}")
    println(s"Memory fraction   : ${config.memoryFractionOverride.getOrElse(spillCase.memoryFraction)}")
    println(s"AQE enabled       : ${spillCase.aqeEnabled}")
    println(s"UI pause          : ${config.uiPauseSeconds} seconds")
    println("============================================================")
  }
}
