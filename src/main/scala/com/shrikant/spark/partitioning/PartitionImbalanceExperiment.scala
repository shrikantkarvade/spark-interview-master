package com.shrikant.spark.partitioning

import com.shrikant.spark.foundation.EnterpriseSparkSession
import org.apache.spark.sql.functions._

object PartitionImbalanceExperiment {

  def main(args: Array[String]): Unit = {

    println()
    println("====================================================")
    println("MODULE 1.5.7 - PARTITION IMBALANCE")
    println("====================================================")

    val spark =
      EnterpriseSparkSession.create(
        appName = "Module 1.5.7 - Partition Imbalance",
        environment = "dev"
      )

    import spark.implicits._

    /*
     * Deliberately create an uneven key distribution.
     *
     * customer_id = 0 represents 50% of the dataset.
     * customer_id = 1..49 represent the remaining 50%.
     */
    val df =
      spark.range(1000000)
        .select(
          when($"id" < 500000, lit(0))
            .when($"id" < 700000, lit(1))
            .when($"id" < 800000, lit(2))
            .when($"id" < 850000, lit(3))
            .when($"id" < 900000, lit(4))
            .otherwise($"id" % 95 + 5)
            .as("distribution_key"),
          $"id"
        )

    println()
    println("====================================================")
    println("INPUT")
    println("====================================================")

    println(
      s"Input partitions = ${df.rdd.getNumPartitions}"
    )

    println(
      s"Input rows = ${df.count()}"
    )

    println()
    println("====================================================")
    println("HASH PARTITIONING")
    println("====================================================")

    val partitioned =
      df.repartition(8, $"distribution_key")

    println(
      s"Output partitions = ${partitioned.rdd.getNumPartitions}"
    )

    println()
    println("====================================================")
    println("ROWS PER PARTITION")
    println("====================================================")

    val distribution =
      partitioned.rdd
        .mapPartitionsWithIndex {
          case (partitionId, rows) =>
            Iterator(
              partitionId -> rows.size
            )
        }
        .collect()
        .sortBy(_._1)

    distribution.foreach {
      case (partitionId, rowCount) =>
        println(
          s"Partition $partitionId -> $rowCount rows"
        )
    }

    println()
    println("====================================================")
    println("KEY DISTRIBUTION")
    println("====================================================")

    df.groupBy($"distribution_key")
      .count()
      .orderBy($"distribution_key")
      .show(20, truncate = false)

    println()
    println("====================================================")
    println("PHYSICAL PLAN")
    println("====================================================")

    partitioned.explain("formatted")

    spark.stop()
  }
}