package com.shrikant.spark.lakehouse

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.log4j.{Level, Logger}
import java.io.File

object MedallionPipelineExperiment {
  def main(args: Array[String]): Unit = {
    // Suppress excessive logging
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)

    val warehouseLocation = new File("spark-warehouse").getAbsolutePath

    val spark = SparkSession.builder()
      .appName("LakehouseMedallionModule5_5")
      .master("local[*]")
      .config("spark.sql.warehouse.dir", warehouseLocation)
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .getOrCreate()

    import spark.implicits._
    val bronzePath = "./tmp/delta-bronze"
    val silverPath = "./tmp/delta-silver"
    val goldPath = "./tmp/delta-gold"

    println("==================================================")
    println("🥉 Hop 1: Landing Raw Data in BRONZE Layer")
    println("==================================================")

    val rawIncomingDF = Seq(
      (1, "Alice", "Engineering", 75000, "2026-10-01"),
      (2, "Bob", "Marketing", 62000, "2026-10-02"),
      (3, "Charlie", "Finance", 70000, "2026-10-03"),
      (1, "Alice", "Engineering", 75000, "2026-10-01") // Duplicate record
    ).toDF("id", "name", "department", "salary", "ingest_date")

    // Bronze: Append-only raw ingestion
    rawIncomingDF.write
      .format("delta")
      .mode("append")
      .save(bronzePath)

    println("Bronze Layer Data:")
    spark.read.format("delta").load(bronzePath).show(false)

    println("==================================================")
    println("🥈 Hop 2: Cleansing & Deduplicating into SILVER Layer")
    println("==================================================")

    val bronzeDF = spark.read.format("delta").load(bronzePath)

    // Silver: Deduplicate and clean
    val silverDF = bronzeDF
      .dropDuplicates("id")
      .filter(col("salary") > 0 && col("name").isNotNull)

    silverDF.write
      .format("delta")
      .mode("overwrite")
      .save(silverPath)

    println("Silver Layer Data (Clean & Deduplicated):")
    spark.read.format("delta").load(silverPath).show(false)

    println("==================================================")
    println("🥇 Hop 3: Aggregating KPIs into GOLD Layer")
    println("==================================================")

    val silverData = spark.read.format("delta").load(silverPath)

    // Gold: Business-level department aggregations
    val goldDF = silverData
      .groupBy("department")
      .agg(
        count("id").as("employee_count"),
        avg("salary").as("avg_salary"),
        sum("salary").as("total_payroll")
      )

    goldDF.write
      .format("delta")
      .mode("overwrite")
      .save(goldPath)

    println("Gold Layer Data (Departmental Analytics):")
    spark.read.format("delta").load(goldPath).show(false)

    println("Medallion Pipeline (Bronze -> Silver -> Gold) completed successfully!")
    spark.stop()
  }
}