package com.shrikant.spark.foundation

import org.apache.spark.sql.SparkSession

object SparkFoundationSQL {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Module 1.2 - Spark Foundation - SQL")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    import spark.implicits._

    println("========================================")
    println(" Module 1.2 - Spark Foundation - SQL")
    println("========================================")

    val transactions = Seq(
      ("T001", "C001", "Pune", 15000.0),
      ("T002", "C002", "Mumbai", 25000.0),
      ("T003", "C001", "Pune", 50000.0),
      ("T004", "C003", "Delhi", 12000.0),
      ("T005", "C002", "Mumbai", 75000.0),
      ("T006", "C001", "Pune", 10000.0),
      ("T007", "C003", "Delhi", 90000.0),
      ("T008", "C002", "Mumbai", 15000.0)
    ).toDF(
      "transaction_id",
      "customer_id",
      "city",
      "amount"
    )

    transactions.createOrReplaceTempView("transactions")

    println()
    println("--- High Value Transactions ---")

    spark.sql(
      """
        |SELECT
        |    transaction_id,
        |    customer_id,
        |    city,
        |    amount
        |FROM transactions
        |WHERE amount >= 50000
        |ORDER BY amount DESC
        |""".stripMargin
    ).show()

    println()
    println("--- Customer Transaction Summary ---")

    val customerSummary = spark.sql(
      """
        |SELECT
        |    customer_id,
        |    SUM(amount) AS total_amount,
        |    COUNT(*) AS transaction_count
        |FROM transactions
        |GROUP BY customer_id
        |ORDER BY total_amount DESC
        |""".stripMargin
    )

    customerSummary.show()

    println()
    println("--- Extended Execution Plan ---")

    customerSummary.explain(true)

    spark.stop()
  }
}