package lakehouse

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import io.delta.tables.DeltaTable
import org.apache.log4j.{Level, Logger}
import java.io.File

object CdcExperiment {
  def main(args: Array[String]): Unit = {
    // Suppress excessive logging
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)

    val warehouseLocation = new File("spark-warehouse").getAbsolutePath

    val spark = SparkSession.builder()
      .appName("LakehouseCdcModule5_2")
      .master("local[*]")
      .config("spark.sql.warehouse.dir", warehouseLocation)
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .getOrCreate()

    import spark.implicits._
    val tablePath = "./tmp/delta-cdc-target"

    println("==================================================")
    println("🚀 Step 1: Initializing Target Table with Base Data")
    println("==================================================")

    val initialEmployees = Seq(
      (1, "Alice", "Engineering", 75000, "active"),
      (2, "Bob", "Marketing", 62000, "active"),
      (3, "Charlie", "Finance", 70000, "active")
    ).toDF("id", "name", "department", "salary", "status")

    // Enable Change Data Feed (CDF) on creation
    initialEmployees.write
      .format("delta")
      .option("delta.enableChangeDataFeed", "true")
      .mode("overwrite")
      .save(tablePath)

    spark.read.format("delta").load(tablePath).show(false)

    println("==================================================")
    println("🔄 Step 2: Simulating Incoming CDC Payload (Upserts & Deletes)")
    println("==================================================")

    // Incoming CDC Stream payload containing:
    // - ID 1: Salary update (Update)
    // - ID 2: Department change (Update)
    // - ID 4: New hire (Insert)
    val cdcUpdates = Seq(
      (1, "Alice", "Engineering", 82000, "active"),  // Update salary
      (2, "Bob", "Sales", 62000, "active"),          // Update department
      (4, "Diana", "HR", 68000, "active")            // Insert new record
    ).toDF("id", "name", "department", "salary", "status")

    val deltaTable = DeltaTable.forPath(spark, tablePath)

    // Apply MERGE INTO (SCD Type 1 Upsert Pattern)
    deltaTable.as("target")
      .merge(
        cdcUpdates.as("source"),
        "target.id = source.id"
      )
      .whenMatched()
      .updateExpr(Map(
        "salary" -> "source.salary",
        "department" -> "source.department",
        "status" -> "source.status"
      ))
      .whenNotMatched()
      .insertExpr(Map(
        "id" -> "source.id",
        "name" -> "source.name",
        "department" -> "source.department",
        "salary" -> "source.salary",
        "status" -> "source.status"
      ))
      .execute()

    println("Target Table State after MERGE INTO Upsert:")
    spark.read.format("delta").load(tablePath).show(false)

    println("==================================================")
    println("📜 Step 3: Reading Change Data Feed (CDF) Stream")
    println("==================================================")

    val cdfDF = spark.read
      .format("delta")
      .option("readChangeFeed", "true")
      .option("startingVersion", 0)
      .load(tablePath)

    println("Captured Row-Level Changes (Inserts, Pre-images, Post-images):")
    cdfDF.select("_change_type", "_commit_version", "id", "name", "salary").show(false)

    spark.stop()
  }
}