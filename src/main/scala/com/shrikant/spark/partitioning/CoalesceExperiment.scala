package com.shrikant.spark.partitioning

import com.shrikant.spark.foundation.EnterpriseSparkSession

object CoalesceExperiment {

  def main(args: Array[String]): Unit = {

    println()
    println("====================================================")
    println("MODULE 1.5.4 - COALESCE")
    println("====================================================")

    val spark =
      EnterpriseSparkSession.create(
        appName = "Module 1.5.4 - Coalesce",
        environment = "dev"
      )

    val df =
      spark.range(1000000)

    println()
    println("====================================================")
    println("BEFORE COALESCE")
    println("====================================================")

    println(
      s"Partitions = ${df.rdd.getNumPartitions}"
    )

    println()
    println("====================================================")
    println("AFTER COALESCE(8)")
    println("====================================================")

    val coalesced =
      df.coalesce(1)

    println(
      s"Partitions = ${coalesced.rdd.getNumPartitions}"
    )

    println()
    println("====================================================")
    println("ROWS PER PARTITION")
    println("====================================================")

    val rowsPerPartition =
      coalesced.rdd
        .mapPartitionsWithIndex {
          case (partitionId, rows) =>
            Iterator(
              partitionId -> rows.size
            )
        }
        .collect()
        .sortBy(_._1)

    rowsPerPartition.foreach {
      case (partitionId, rowCount) =>
        println(
          s"Partition $partitionId -> $rowCount rows"
        )
    }

    println()
    println("====================================================")
    println("PHYSICAL PLAN")
    println("====================================================")

    coalesced.explain("formatted")

    spark.stop()
  }
}