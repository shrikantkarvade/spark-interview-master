package com.shrikant.spark.foundation

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object SparkFoundation {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Module 1.2 - Spark Foundation")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    import spark.implicits._

    println("========================================")
    println("      Module 1.2 - Spark Foundation")
    println("========================================")

    // ------------------------------------------------------------
    // 1. Create sample transaction data
    // ------------------------------------------------------------

    val transactions = Seq(
      ("T001", "C001", "Pune", 15000.0),
      ("T002", "C002", "Mumbai", 25000.0),
      ("T003", "C001", "Pune", 50000.0),
      ("T004", "C003", "Delhi", 12000.0),
      ("T005", "C002", "Mumbai", 75000.0),
      ("T006", "C001", "Pune", 10000.0),
      ("T007", "C003", "Delhi", 90000.0),
      ("T008", "C002", "Mumbai", 15000.0)
    )

    val transactionsDf = transactions.toDF(
      "transaction_id",
      "customer_id",
      "city",
      "amount"
    )

    println("\n--- Source Data ---")

    transactionsDf.show()

    // ------------------------------------------------------------
    // 2. Transformation
    // ------------------------------------------------------------

    println("\n--- Defining Transformation ---")

    val highValueTransactions = transactionsDf
      .filter(col("amount") >= 50000)

    println("Transformation defined.")
    println("No Spark computation has been triggered yet.")

    // ------------------------------------------------------------
    // 3. Action
    // ------------------------------------------------------------

    println("\n--- Executing Action ---")

    highValueTransactions.show()

    // ------------------------------------------------------------
    // 4. Aggregation
    // ------------------------------------------------------------

    println("\n--- Total Transaction Amount By Customer ---")

    val customerTotals = transactionsDf
      .groupBy("customer_id")
      .agg(
        sum("amount").alias("total_amount"),
        count("*").alias("transaction_count")
      )
      .orderBy(desc("total_amount"))

    customerTotals.show()

    // ------------------------------------------------------------
    // 5. Explain the execution plan
    // ------------------------------------------------------------

    println("\n--- Extended Execution Plan ---")

    customerTotals.explain(true)

    spark.stop()
  }
}