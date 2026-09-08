package com.shrikant.spark.partitioning

import com.shrikant.spark.foundation.EnterpriseSparkSession

object PartitionBaselineExperiment {

  def main(args: Array[String]): Unit = {

    println()
    println("====================================================")
    println("MODULE 1.5.1 - PARTITION BASELINE")
    println("====================================================")

    val spark =
      EnterpriseSparkSession.create(
        appName = "Module 1.5.1 - Partition Baseline",
        environment = "dev"
      )

    import spark.implicits._

    val df =
      spark.range(1000000)

    println()
    println("====================================================")
    println("SPARK CONFIGURATION")
    println("====================================================")

    println(
      s"spark.master = ${spark.sparkContext.master}"
    )

    println(
      s"defaultParallelism = ${spark.sparkContext.defaultParallelism}"
    )

    println()
    println("====================================================")
    println("DATAFRAME PARTITIONING")
    println("====================================================")

    println(
      s"Number of partitions = ${df.rdd.getNumPartitions}"
    )

    println()
    println("====================================================")
    println("ROWS PER PARTITION")
    println("====================================================")

    val rowsPerPartition =
      df.rdd
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

    df.explain("formatted")

    spark.stop()
  }
}