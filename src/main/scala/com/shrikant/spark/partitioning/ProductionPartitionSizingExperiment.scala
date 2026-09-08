package com.shrikant.spark.partitioning

import com.shrikant.spark.foundation.EnterpriseSparkSession
import org.apache.spark.sql.functions._

object ProductionPartitionSizingExperiment {

  private val RowCount = 10000000L
  private val CustomerCount = 1000000L

  private val PartitionCounts =
    Seq(2, 4, 8, 16, 32, 64)

  private val WarmupRuns = 1
  private val MeasuredRuns = 5

  private case class BenchmarkResult(
                                      partitionCount: Int,
                                      resultRows: Long,
                                      minimumMs: Long,
                                      medianMs: Long,
                                      maximumMs: Long
                                    )

  def main(args: Array[String]): Unit = {

    println("=" * 60)
    println("MODULE 1.5.10 - PRODUCTION PARTITION SIZING")
    println("=" * 60)

    println(s"Spark ${org.apache.spark.SPARK_VERSION}")
    println(s"Windows ${System.getProperty("os.version")}")
    println(s"Java ${System.getProperty("java.version")}")
    println()

    val spark =
      EnterpriseSparkSession.create(
        appName = "Module 1.5.10 - Production Partition Sizing",
        environment = "dev"
      )

    try {

      spark.sparkContext.setLogLevel("WARN")

      /*
       * AQE is intentionally enabled for this experiment.
       *
       * The benchmark evaluates different initial shuffle
       * partition counts while Spark is allowed to adapt
       * downstream shuffle processing using runtime statistics.
       */
      spark.conf.set(
        "spark.sql.adaptive.enabled",
        true
      )

      spark.conf.set(
        "spark.sql.adaptive.coalescePartitions.enabled",
        true
      )

      spark.conf.set(
        "spark.sql.adaptive.advisoryPartitionSizeInBytes",
        1 * 1024 * 1024
      )

      println(
        s"spark.master = ${spark.conf.get("spark.master")}"
      )

      println(
        s"defaultParallelism = " +
          s"${spark.sparkContext.defaultParallelism}"
      )

      println(
        s"Input rows = $RowCount"
      )

      println(
        s"Distinct customers = $CustomerCount"
      )

      println(
        s"spark.sql.adaptive.enabled = " +
          s"${spark.conf.get("spark.sql.adaptive.enabled")}"
      )

      println(
        s"spark.sql.adaptive.coalescePartitions.enabled = " +
          s"${spark.conf.get(
            "spark.sql.adaptive.coalescePartitions.enabled"
          )}"
      )

      println(
        s"spark.sql.adaptive.advisoryPartitionSizeInBytes = " +
          s"${spark.conf.get(
            "spark.sql.adaptive.advisoryPartitionSizeInBytes"
          )}"
      )

      println()

      /*
       * Create a deterministic transaction-style dataset.
       *
       * The dataset is cached and materialized before benchmarking
       * so that dataset generation is excluded from each scenario.
       */
      val transactions =
        spark.range(RowCount)
          .select(
            col("id")
              .alias("transaction_id"),

            (
              pmod(
                col("id"),
                lit(CustomerCount)
              ) +
                lit(1)
              )
              .cast("string")
              .alias("customer_id"),

            when(
              pmod(col("id"), lit(5)) === 0,
              "Pune"
            )
              .when(
                pmod(col("id"), lit(5)) === 1,
                "Mumbai"
              )
              .when(
                pmod(col("id"), lit(5)) === 2,
                "Delhi"
              )
              .when(
                pmod(col("id"), lit(5)) === 3,
                "Bangalore"
              )
              .otherwise("Hyderabad")
              .alias("city"),

            (
              pmod(
                col("id") * lit(7919L),
                lit(990000L)
              ) +
                lit(10000L)
              )
              .cast("double")
              .alias("amount")
          )
          .cache()

      val materializationStart =
        System.nanoTime()

      transactions.count()

      val materializationMs =
        (System.nanoTime() - materializationStart) / 1000000

      println(
        f"Dataset materialization time = " +
          f"${materializationMs / 1000.0}%.2f seconds"
      )

      println()

      /*
       * Benchmark each candidate shuffle partition count.
       *
       * Only the initial shuffle partition count changes between
       * scenarios. The input dataset and aggregation remain identical.
       */
      val benchmarkResults =
        PartitionCounts.map { partitionCount =>

          println("=" * 60)
          println(
            s"SHUFFLE PARTITIONS = $partitionCount"
          )
          println("=" * 60)

          spark.conf.set(
            "spark.sql.shuffle.partitions",
            partitionCount
          )

          val aggregation =
            transactions
              .groupBy(col("customer_id"))
              .agg(
                sum("amount")
                  .alias("total_amount"),

                count("*")
                  .alias("transaction_count")
              )

          /*
           * Warm-up.
           *
           * Excluded from benchmark statistics.
           */
          (1 to WarmupRuns).foreach { run =>

            val start =
              System.nanoTime()

            val resultRows =
              aggregation.count()

            val elapsedMs =
              (System.nanoTime() - start) / 1000000

            require(
              resultRows == CustomerCount,
              s"Expected $CustomerCount result rows " +
                s"but got $resultRows"
            )

            println(
              s"Warm-up $run -> $elapsedMs ms"
            )
          }

          /*
           * Measured runs.
           */
          val times =
            scala.collection.mutable.ArrayBuffer.empty[Long]

          var resultRows = 0L

          (1 to MeasuredRuns).foreach { run =>

            val start =
              System.nanoTime()

            resultRows =
              aggregation.count()

            val elapsedMs =
              (System.nanoTime() - start) / 1000000

            require(
              resultRows == CustomerCount,
              s"Expected $CustomerCount result rows " +
                s"but got $resultRows"
            )

            times += elapsedMs

            println(
              s"Run $run -> $elapsedMs ms"
            )
          }

          val sortedTimes =
            times.sorted

          val minimum =
            sortedTimes.head

          val maximum =
            sortedTimes.last

          val median =
            sortedTimes(sortedTimes.size / 2)

          /*
           * Capture the executed-plan snapshot once.
           *
           * Spark 3.5.1 may report isFinalPlan=false for this
           * benchmark even after the action. Therefore this snapshot
           * is retained only as diagnostic information and is not
           * interpreted as proof of the final AQE plan.
           */
          val executedPlanSnapshot =
            aggregation.queryExecution.executedPlan.toString

          val adaptivePlan =
            executedPlanSnapshot.contains(
              "AdaptiveSparkPlan"
            )

          println()
          println(
            s"Executed plan contains AdaptiveSparkPlan = $adaptivePlan"
          )

          println()
          println(s"Result rows = $resultRows")
          println(s"Minimum execution time = $minimum ms")
          println(s"Median execution time = $median ms")
          println(s"Maximum execution time = $maximum ms")
          println()

          BenchmarkResult(
            partitionCount = partitionCount,
            resultRows = resultRows,
            minimumMs = minimum,
            medianMs = median,
            maximumMs = maximum
          )
        }

      /*
       * Benchmark summary.
       */
      println()
      println("=" * 60)
      println("BENCHMARK SUMMARY")
      println("=" * 60)

      println(
        "Partitions | Result Rows | Min (ms) | Median (ms) | Max (ms)"
      )

      println(
        "-----------|-------------|----------|-------------|---------"
      )

      benchmarkResults.foreach { result =>

        println(
          f"${result.partitionCount}%10d | " +
            f"${result.resultRows}%11d | " +
            f"${result.minimumMs}%8d | " +
            f"${result.medianMs}%11d | " +
            f"${result.maximumMs}%8d"
        )
      }

      val fastest =
        benchmarkResults.minBy(_.medianMs)

      println()

      println(
        s"Fastest configuration by median = " +
          s"${fastest.partitionCount} partitions"
      )

      println(
        s"Fastest median execution time = " +
          s"${fastest.medianMs} ms"
      )

      println()

      println(
        "Benchmark methodology:"
      )

      println(
        s"- Warm-up runs per scenario = $WarmupRuns"
      )

      println(
        s"- Measured runs per scenario = $MeasuredRuns"
      )

      println(
        "- Primary metric = median execution time"
      )

      println(
        "- Input dataset was cached before benchmarking"
      )

      println(
        "- AQE enabled with shuffle partition coalescing enabled"
      )

      println()

      println(
        "Production sizing experiment completed."

      )

    } finally {

      spark.stop()
    }
  }
}
