package com.shrikant.spark.partitioning

import com.shrikant.spark.foundation.EnterpriseSparkSession
import org.apache.spark.sql.functions._

object DataSkewMitigationExperiment {

  private val NumRows = 1000000
  private val NumPartitions = 8
  private val NumSalts = 8

  def main(args: Array[String]): Unit = {

    println()
    println("====================================================")
    println("MODULE 1.5.8 - DATA SKEW MITIGATION")
    println("====================================================")

    val spark =
      EnterpriseSparkSession.create(
        appName = "Module 1.5.8 - Data Skew Mitigation",
        environment = "dev"
      )

    import spark.implicits._

    /*
     * Reuse the same deliberately skewed distribution
     * from Module 1.5.7.
     *
     * Key 0  -> 500,000 rows
     * Key 1  -> 200,000 rows
     * Key 2  -> 100,000 rows
     * Key 3  ->  50,000 rows
     * Key 4  ->  50,000 rows
     * Keys 5-99 -> remaining 100,000 rows
     */
    val df =
      spark.range(NumRows)
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
    println("BASELINE")
    println("====================================================")

    val baseline =
      df.repartition(
        NumPartitions,
        $"distribution_key"
      )

    println(
      s"Baseline partitions = ${baseline.rdd.getNumPartitions}"
    )

    val baselineDistribution =
      baseline.rdd
        .mapPartitionsWithIndex {
          case (partitionId, rows) =>
            Iterator(
              partitionId -> rows.size
            )
        }
        .collect()
        .sortBy(_._1)

    baselineDistribution.foreach {
      case (partitionId, rowCount) =>
        println(
          s"Partition $partitionId -> $rowCount rows"
        )
    }

    println()
    println("Baseline physical plan:")
    baseline.explain("formatted")

    /*
     * Salt only the hot key.
     *
     * The hot key (0) is spread across multiple salt values.
     * Other keys retain salt = 0.
     *
     * This changes the partitioning key from:
     *
     *     distribution_key
     *
     * to:
     *
     *     (distribution_key, salt)
     */
    println()
    println("====================================================")
    println("SALTED KEY")
    println("====================================================")

    val salted =
      df.withColumn(
        "salt",
        when(
          $"distribution_key" === 0,
          pmod($"id", lit(NumSalts))
        ).otherwise(lit(0))
      )

    println()
    println("Salt distribution for hot key:")
    salted
      .filter($"distribution_key" === 0)
      .groupBy($"salt")
      .count()
      .orderBy($"salt")
      .show(truncate = false)

    println()
    println("====================================================")
    println("HASH PARTITIONING WITH SALT")
    println("====================================================")

    val saltedPartitioned =
      salted.repartition(
        NumPartitions,
        $"distribution_key",
        $"salt"
      )

    println(
      s"Salted partitions = ${saltedPartitioned.rdd.getNumPartitions}"
    )

    val saltedDistribution =
      saltedPartitioned.rdd
        .mapPartitionsWithIndex {
          case (partitionId, rows) =>
            Iterator(
              partitionId -> rows.size
            )
        }
        .collect()
        .sortBy(_._1)

    saltedDistribution.foreach {
      case (partitionId, rowCount) =>
        println(
          s"Partition $partitionId -> $rowCount rows"
        )
    }

    println()
    println("Salted physical plan:")
    saltedPartitioned.explain("formatted")

    println()
    println("====================================================")
    println("CORRECTNESS")
    println("====================================================")

    val baselineCount =
      baseline.count()

    val saltedCount =
      saltedPartitioned.count()

    println(
      s"Baseline row count = $baselineCount"
    )

    println(
      s"Salted row count   = $saltedCount"
    )

    println(
      s"Counts equal       = ${baselineCount == saltedCount}"
    )

    spark.stop()
  }
}
