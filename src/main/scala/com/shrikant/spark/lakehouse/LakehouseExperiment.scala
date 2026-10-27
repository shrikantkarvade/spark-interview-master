package lakehouse

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import io.delta.tables.DeltaTable
import org.apache.log4j.{Level, Logger}
import java.io.File

object LakehouseExperiment {
  def main(args: Array[String]): Unit = {
    // Suppress excessive logging for clean output
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)

    // Configure Windows Hadoop home if needed
    val warehouseLocation = new File("spark-warehouse").getAbsolutePath
    
    val spark = SparkSession.builder()
      .appName("LakehouseArchitectureModule5_1")
      .master("local[*]")
      .config("spark.sql.warehouse.dir", warehouseLocation)
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .getOrCreate()

    import spark.implicits._

    val tablePath = "./tmp/delta-transactions"

    println("==================================================")
    println("🚀 Step 1: Writing Initial Delta Dataset (v0)")
    println("==================================================")

    val initialData = Seq(
      (1, "Alice", "Engineering", 75000, "2026-01-15"),
      (2, "Bob", "Marketing", 62000, "2026-02-10"),
      (3, "Charlie", "Engineering", 81000, "2026-03-01")
    ).toDF("id", "name", "department", "salary", "join_date")

    initialData.write
      .format("delta")
      .mode("overwrite")
      .save(tablePath)

    spark.read.format("delta").load(tablePath).show(false)

    println("==================================================")
    println("🔄 Step 2: Performing ACID Update & Insert (v1 & v2)")
    println("==================================================")

    val deltaTable = DeltaTable.forPath(spark, tablePath)

    // Update Charlie's salary (ACID Update)
    deltaTable.update(
      col("id") === 3,
      Map("salary" -> lit(85000))
    )

    // Append new employee (ACID Insert)
    val newData = Seq(
      (4, "Diana", "Finance", 70000, "2026-03-15")
    ).toDF("id", "name", "department", "salary", "join_date")

    newData.write
      .format("delta")
      .mode("append")
      .save(tablePath)

    println("Current Table State after Updates/Appends:")
    spark.read.format("delta").load(tablePath).show(false)

    println("==================================================")
    println("⏳ Step 3: Querying History & Time Travel")
    println("==================================================")

    val historyDF = deltaTable.history()
    println("Delta Transaction Log History:")
    historyDF.select("version", "operation", "operationMetrics", "timestamp").show(false)

    println("Time Travel: Querying Version 0 (Original State):")
    val v0DF = spark.read
      .format("delta")
      .option("versionAsOf", 0)
      .load(tablePath)
    v0DF.show(false)

    println("==================================================")
    println("🧹 Step 4: Maintenance (OPTIMIZE & VACUUM)")
    println("==================================================")

    // Compact small files
    deltaTable.optimize().executeCompaction()
    println("Table optimized via file compaction.")

    // Disable the retention duration safety check for local testing/vacuuming with 0 hours
    spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")

    // Vacuum old historical files (retaining 0 hours for local testing, default is 168 hours)
    spark.conf.set("spark.databricks.delta.vacuum.parallelDelete.enabled", "true")
    deltaTable.vacuum(0.0) // WARNING: In production, use retention > 168 hours to support concurrent readers/time travel.
    println("Table vacuumed successfully.")

    spark.stop()
  }
}