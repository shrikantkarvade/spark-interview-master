package com.shrikant.spark.foundation

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object SparkFoundationPerformance {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("Module 1.2 - Spark Performance Investigation")
      .master("local[*]")
      .config("spark.sql.shuffle.partitions", "8")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    import spark.implicits._

    val transactions = Seq(
      ("T001", "C001", "Pune",   15000.0),
      ("T002", "C002", "Mumbai", 25000.0),
      ("T003", "C001", "Pune",   50000.0),
      ("T004", "C003", "Delhi",  12000.0),
      ("T005", "C002", "Mumbai", 75000.0),
      ("T006", "C001", "Pune",   10000.0),
      ("T007", "C003", "Delhi",  90000.0),
      ("T008", "C002", "Mumbai", 15000.0)
    ).toDF(
      "transaction_id",
      "customer_id",
      "city",
      "amount"
    )

    println("\n====================================================")
    println("BASELINE: GROUP BY + AGGREGATION + ORDER BY")
    println("====================================================")

    val baseline = transactions
      .groupBy("customer_id")
      .agg(
        sum("amount").alias("total_amount"),
        count("*").alias("transaction_count")
      )
      .orderBy(desc("total_amount"))

    baseline.show(false)

    println("\n--- BASELINE PHYSICAL PLAN ---")
    baseline.explain(true)

    println("\n====================================================")
    println("INVESTIGATION: AGGREGATION WITHOUT ORDER BY")
    println("====================================================")

    val aggregationOnly = transactions
      .groupBy("customer_id")
      .agg(
        sum("amount").alias("total_amount"),
        count("*").alias("transaction_count")
      )

    aggregationOnly.show(false)

    println("\n--- AGGREGATION-ONLY PHYSICAL PLAN ---")
    aggregationOnly.explain(true)

    println("\n====================================================")
    println("INVESTIGATION: TOP-N INSTEAD OF GLOBAL ORDER")
    println("====================================================")

    val topCustomers = transactions
      .groupBy("customer_id")
      .agg(
        sum("amount").alias("total_amount"),
        count("*").alias("transaction_count")
      )
      .orderBy(desc("total_amount"))
      .limit(2)

    topCustomers.show(false)

    println("\n--- TOP-N PHYSICAL PLAN ---")
    topCustomers.explain(true)

    println("\n====================================================")
    println("PERFORMANCE INVESTIGATION COMPLETE")
    println("====================================================")

    spark.stop()
  }
}