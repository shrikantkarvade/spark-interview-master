package lakehouse

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.log4j.{Level, Logger}
import java.io.File

object DataQualityExperiment {
  def main(args: Array[String]): Unit = {
    // Suppress excessive logging
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)

    val warehouseLocation = new File("spark-warehouse").getAbsolutePath

    val spark = SparkSession.builder()
      .appName("LakehouseDataQualityModule5_3")
      .master("local[*]")
      .config("spark.sql.warehouse.dir", warehouseLocation)
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .getOrCreate()

    import spark.implicits._
    val cleanTablePath = "./tmp/delta-silver-clean"
    val dlqTablePath = "./tmp/delta-dlq"

    println("==================================================")
    println("🚀 Step 1: Ingesting Raw Data with Validation Anomalies")
    println("==================================================")

    // Simulating raw input containing deliberate data quality errors
    val rawInputDF = Seq(
      (Some(1), Some("Alice"), Some("Engineering"), Some(75000), Some("active")),
      (Some(2), None, Some("Marketing"), Some(62000), Some("active")),        // Error: Missing name
      (Some(3), Some("Charlie"), Some("Finance"), Some(-5000), Some("active")), // Error: Negative salary
      (Some(4), Some("Diana"), Some("HR"), Some(70000), Some("unknown")),      // Error: Invalid status code
      (None, Some("Eve"), Some("Operations"), Some(65000), Some("active"))     // Error: Missing ID
    ).toDF("id", "name", "department", "salary", "status")

    println("Raw Input Data:")
    rawInputDF.show(false)

    println("==================================================")
    println("🛡️ Step 2: Applying Quality Expectations & DLQ Split")
    println("==================================================")

    // Define strict data quality expectation rules
    val isValidCondition = col("id").isNotNull &&
                           col("name").isNotNull &&
                           col("salary") > 0 &&
                           col("status").isin("active", "inactive")

    // Split dataset into valid (clean) and invalid (quarantined) streams
    val validDF = rawInputDF.filter(isValidCondition)
    
    val quarantineDF = rawInputDF.filter(not(isValidCondition))
      .withColumn("failed_at_timestamp", current_timestamp())
      .withColumn("error_reason", 
        expr("""
          CASE 
            WHEN id IS NULL THEN 'Missing ID'
            WHEN name IS NULL THEN 'Missing Name'
            WHEN salary <= 0 THEN 'Invalid Salary (<= 0)'
            WHEN status NOT IN ('active', 'inactive') THEN 'Invalid Status Code'
            ELSE 'Unknown Quality Violation'
          END
        """)
      )

    println("--- Valid Records (Proceeding to Silver Layer) ---")
    validDF.show(false)

    println("--- Quarantined Records (Dead Letter Queue - DLQ) ---")
    quarantineDF.show(false)

    println("==================================================")
    println("💾 Step 3: Committing to Delta Clean & DLQ Tables")
    println("==================================================")

    validDF.write
      .format("delta")
      .mode("overwrite")
      .save(cleanTablePath)

    quarantineDF.write
      .format("delta")
      .mode("overwrite")
      .save(dlqTablePath)

    println("Data quality expectations enforced and tables persisted successfully!")
    spark.stop()
  }
}