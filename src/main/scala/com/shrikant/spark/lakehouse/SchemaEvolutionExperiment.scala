package lakehouse

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.log4j.{Level, Logger}
import java.io.File

object SchemaEvolutionExperiment {
  def main(args: Array[String]): Unit = {
    // Suppress excessive logging
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)

    val warehouseLocation = new File("spark-warehouse").getAbsolutePath

    val spark = SparkSession.builder()
      .appName("LakehouseSchemaEvolutionModule5_4")
      .master("local[*]")
      .config("spark.sql.warehouse.dir", warehouseLocation)
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .getOrCreate()

    import spark.implicits._
    val tablePath = "./tmp/delta-schema-evolution"

    println("==================================================")
    println("🚀 Step 1: Writing Initial Table (Schema Version 1)")
    println("==================================================")

    val v1Data = Seq(
      (1, "Alice", "Engineering"),
      (2, "Bob", "Marketing")
    ).toDF("id", "name", "department")

    v1Data.write
      .format("delta")
      .mode("overwrite")
      .save(tablePath)

    println("Target Table Schema (v1):")
    spark.read.format("delta").load(tablePath).printSchema()
    spark.read.format("delta").load(tablePath).show(false)

    println("==================================================")
    println("🔄 Step 2: Ingesting Evolved Data (Schema Version 2 - Additive)")
    println("==================================================")

    // New incoming batch contains extra columns: 'salary' (Integer) and 'email' (String)
    val v2Data = Seq(
      (3, "Charlie", "Finance", 80000, "charlie@company.com"),
      (4, "Diana", "HR", 72000, "diana@company.com")
    ).toDF("id", "name", "department", "salary", "email")

    println("Attempting append with schema evolution enabled (.option(\"mergeSchema\", \"true\"))...")

    v2Data.write
      .format("delta")
      .mode("append")
      .option("mergeSchema", "true")
      .save(tablePath)

    println("Successfully evolved schema and appended data!")
    println("Evolved Table Schema (v2):")
    spark.read.format("delta").load(tablePath).printSchema()

    println("Final Evolved Table Content:")
    spark.read.format("delta").load(tablePath).show(false)

    spark.stop()
  }
}