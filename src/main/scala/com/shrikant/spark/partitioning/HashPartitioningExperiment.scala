package com.shrikant.spark.partitioning

import com.shrikant.spark.foundation.EnterpriseSparkSession

object HashPartitioningExperiment {

  def main(args: Array[String]): Unit = {

    println()
    println("====================================================")
    println("MODULE 1.5.6 - HASH PARTITIONING")
    println("====================================================")

    val spark =
      EnterpriseSparkSession.create(
        appName = "Module 1.5.6 - Hash Partitioning",
        environment = "dev"
      )

    import spark.implicits._

    val df =
      spark.range(1000000)
        .select(
          ($"id" % 100).as("customer_id"),
          $"id"
        )

    println()
    println("====================================================")
    println("BASELINE")
    println("====================================================")

    println(
      s"Input partitions = ${df.rdd.getNumPartitions}"
    )

    println()
    println("====================================================")
    println("REPARTITION(8) - ROUND ROBIN")
    println("====================================================")

    val roundRobin =
      df.repartition(8)

    println(
      s"Partitions = ${roundRobin.rdd.getNumPartitions}"
    )

    val roundRobinDistribution =
      roundRobin.rdd
        .mapPartitionsWithIndex {
          case (partitionId, rows) =>
            Iterator(
              partitionId -> rows.size
            )
        }
        .collect()
        .sortBy(_._1)

    roundRobinDistribution.foreach {
      case (partitionId, rowCount) =>
        println(
          s"Partition $partitionId -> $rowCount rows"
        )
    }

    println()
    println("Physical plan:")
    roundRobin.explain("formatted")

    println()
    println("====================================================")
    println("REPARTITION(8, customer_id) - HASH")
    println("====================================================")

    val hashPartitioned =
      df.repartition(8, $"customer_id")

    println(
      s"Partitions = ${hashPartitioned.rdd.getNumPartitions}"
    )

    val hashDistribution =
      hashPartitioned.rdd
        .mapPartitionsWithIndex {
          case (partitionId, rows) =>
            Iterator(
              partitionId -> rows.size
            )
        }
        .collect()
        .sortBy(_._1)

    hashDistribution.foreach {
      case (partitionId, rowCount) =>
        println(
          s"Partition $partitionId -> $rowCount rows"
        )
    }

    println()
    println("Physical plan:")
    hashPartitioned.explain("formatted")

    println()
    println("====================================================")
    println("KEY DISTRIBUTION")
    println("====================================================")

    val keyDistribution =
      hashPartitioned
        .groupBy($"customer_id")
        .count()
        .orderBy($"customer_id")

    keyDistribution.show(20, truncate = false)

    spark.stop()
  }
}