package lakehouse

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger
import org.apache.log4j.{Level, Logger}
import java.io.File

object StreamingLakehouseExperiment {
  def main(args: Array[String]): Unit = {
    // Suppress excessive logging
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)

    val warehouseLocation = new File("spark-warehouse").getAbsolutePath

    val spark = SparkSession.builder()
      .appName("LakehouseStreamingModule5_6")
      .master("local[*]")
      .config("spark.sql.warehouse.dir", warehouseLocation)
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .getOrCreate()

    val streamTablePath = "./tmp/delta-streaming-sink"
    val checkpointPath = "./tmp/delta-streaming-checkpoint"

    println("==================================================")
    println("🚀 Step 1: Configuring Spark Structured Streaming Source")
    println("==================================================")

    // Using Spark's built-in rate source to simulate incoming real-time telemetry (5 rows per second)
    val streamingInputDF = spark.readStream
      .format("rate")
      .option("rowsPerSecond", 5)
      .load()

    println("==================================================")
    println("🔄 Step 2: Writing Streaming Micro-Batches to Delta Sink")
    println("==================================================")

    val query = streamingInputDF
      .selectExpr("timestamp", "value AS event_id", "value % 10 AS sensor_id")
      .writeStream
      .format("delta")
      .outputMode("append")
      .option("checkpointLocation", checkpointPath)
      .trigger(Trigger.ProcessingTime("5 seconds"))
      .start(streamTablePath)

    println("Streaming query started. Ingesting micro-batches into Delta table...")
    
    // Let the stream run for 15 seconds to capture a few micro-batches
    Thread.sleep(15000)

    println("==================================================")
    println("📊 Step 3: Inspecting Resulting Delta Stream Table")
    println("==================================================")

    val resultingDF = spark.read.format("delta").load(streamTablePath)
    println(s"Total records captured in Delta sink: {resultingDF.count()}")
    resultingDF.show(10, false)

    // Gracefully stop the streaming query
    query.stop()
    println("Streaming query stopped successfully.")
    spark.stop()
  }
}