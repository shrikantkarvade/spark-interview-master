package com.shrikant.spark.partitioning

import com.shrikant.spark.foundation.EnterpriseSparkSession

object RepartitionExperiment {

  def main(args: Array[String]): Unit = {

    println()
    println("====================================================")
    println("MODULE 1.5.3 - REPARTITION")
    println("====================================================")

    val spark =
      EnterpriseSparkSession.create(
        appName = "Module 1.5.3 - Repartition",
        environment = "dev"
      )

    val df =
      spark.range(1000000)

    println()
    println("====================================================")
    println("BEFORE REPARTITION")
    println("====================================================")

    println(
      s"Partitions = ${df.rdd.getNumPartitions}"
    )

    println()
    println("====================================================")
    println("AFTER REPARTITION")
    println("====================================================")

    val repartitioned =
      df.repartition(8)

    println(
      s"Partitions = ${repartitioned.rdd.getNumPartitions}"
    )

    println()
    println("====================================================")
    println("ROWS PER PARTITION")
    println("====================================================")

    val rowsPerPartition =
      repartitioned.rdd
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

    repartitioned.explain("formatted")

    spark.stop()
  }
}