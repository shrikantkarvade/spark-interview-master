package com.shrikant.spark.foundation

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.junit.{After, Before, Test}

import org.junit.Assert._

class SparkFoundationTest {

  private var spark: SparkSession = _

  @Before
  def setUp(): Unit = {
    spark = SparkSession
      .builder()
      .appName("Module 1.2 - Spark Foundation Tests")
      .master("local[2]")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.shuffle.partitions", "2")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
  }

  @After
  def tearDown(): Unit = {
    if (spark != null) {
      spark.stop()
      spark = null
    }
  }

  private def createTransactions(): DataFrame = {
    val sparkSession = spark
    import sparkSession.implicits._

    Seq(
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
  }

  @Test
  def shouldFilterHighValueTransactions(): Unit = {
    val transactions = createTransactions()

    val highValueTransactions = transactions
      .filter(col("amount") >= 50000)

    assertEquals(
      "Expected three high-value transactions",
      3,
      highValueTransactions.count()
    )

    assertTrue(
      "Every transaction should have amount >= 50000",
      highValueTransactions
        .select("amount")
        .collect()
        .forall(row => row.getAs[Double]("amount") >= 50000)
    )
  }

  @Test
  def shouldCalculateCustomerTransactionSummary(): Unit = {
    val transactions = createTransactions()

    val summary = transactions
      .groupBy("customer_id")
      .agg(
        sum("amount").alias("total_amount"),
        count("*").alias("transaction_count")
      )
      .collect()
      .map { row =>
        row.getAs[String]("customer_id") ->
          (
            row.getAs[Double]("total_amount"),
            row.getAs[Long]("transaction_count")
          )
      }
      .toMap

    assertEquals((115000.0, 3L), summary("C002"))
    assertEquals((102000.0, 2L), summary("C003"))
    assertEquals((75000.0, 3L), summary("C001"))
  }

  @Test
  def shouldReturnExpectedCustomerCount(): Unit = {
    val transactions = createTransactions()

    assertEquals(
      "Expected three unique customers",
      3,
      transactions
        .select("customer_id")
        .distinct()
        .count()
    )
  }

  @Test
  def shouldExposeExpectedSchema(): Unit = {
    val transactions = createTransactions()

    assertEquals(
      Seq("transaction_id", "customer_id", "city", "amount"),
      transactions.columns.toSeq
    )

    assertEquals(
      "double",
      transactions.schema("amount").dataType.typeName
    )
  }

  @Test
  def shouldProduceExpectedSqlResult(): Unit = {
    val transactions = createTransactions()

    transactions.createOrReplaceTempView("transactions")

    val result = spark.sql(
      """
        SELECT transaction_id, customer_id, city, amount
        FROM transactions
        WHERE amount >= 50000
        ORDER BY amount DESC
      """
    )

    val transactionIds = result
      .select("transaction_id")
      .collect()
      .map(_.getAs[String]("transaction_id"))
      .toSeq

    assertEquals(
      Seq("T007", "T005", "T003"),
      transactionIds
    )
  }
}