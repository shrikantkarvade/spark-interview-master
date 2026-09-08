package com.shrikant.spark.partitioning

import com.shrikant.spark.foundation.EnterpriseSparkSession

object PartitionChangeBenchmark {

  def main(args: Array[String]): Unit = {

    println()
    println("====================================================")
    println("MODULE 1.5.5 - REPARTITION VS COALESCE")
    println("====================================================")

    val spark =
      EnterpriseSparkSession.create(
        appName = "Module 1.5.5 - Repartition vs Coalesce",
        environment = "dev"
      )

    val df =
      spark.range(1000000)

    println()
    println("====================================================")
    println("BASELINE")
    println("====================================================")

    println(
      s"Baseline partitions = ${df.rdd.getNumPartitions}"
    )

    println()
    println("====================================================")
    println("REPARTITION(8)")
    println("====================================================")

    val repartitioned =
      df.repartition(8)

    val repartitionStart =
      System.nanoTime()

    val repartitionCount =
      repartitioned.count()

    val repartitionTime =
      (System.nanoTime() - repartitionStart) / 1000000

    println(
      s"Partitions = ${repartitioned.rdd.getNumPartitions}"
    )

    println(
      s"Rows = $repartitionCount"
    )

    println(
      s"Execution time = ${repartitionTime} ms"
    )

    println()
    println("Physical plan:")
    repartitioned.explain("formatted")

    println()
    println("====================================================")
    println("COALESCE(1)")
    println("====================================================")

    val coalesced =
      df.coalesce(1)

    val coalesceStart =
      System.nanoTime()

    val coalesceCount =
      coalesced.count()

    val coalesceTime =
      (System.nanoTime() - coalesceStart) / 1000000

    println(
      s"Partitions = ${coalesced.rdd.getNumPartitions}"
    )

    println(
      s"Rows = $coalesceCount"
    )

    println(
      s"Execution time = ${coalesceTime} ms"
    )

    println()
    println("Physical plan:")
    coalesced.explain("formatted")

    println()
    println("====================================================")
    println("BENCHMARK SUMMARY")
    println("====================================================")

    println(
      s"Repartition(8) time = ${repartitionTime} ms"
    )

    println(
      s"Coalesce(1) time    = ${coalesceTime} ms"
    )

    spark.stop()
  }
}