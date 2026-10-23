package streaming

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{OutputMode, Trigger}

/**
 * Module 4.2: State Management, RocksDB State Store & Watermarking Mechanics
 * 
 * Demonstrates:
 * 1. Configuring RocksDB State Store provider for production off-heap state management.
 * 2. Event-time watermarking for late-data handling.
 * 3. Stateful windowed aggregation with state eviction and cleanup.
 */
object StateManagementWatermarkingExperiment {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Module4_2_StateManagementAndWatermarking")
      .master("local[2]")
      .config("spark.sql.shuffle.partitions", "2")
      // Production-grade State Store Configuration: RocksDB Provider
      .config("spark.sql.streaming.stateStore.providerClass", "org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider")
      .getOrCreate()

    import spark.implicits._

    println("=== Step 1: Generating Simulated Event Stream ===")
    
    // Simulate incoming IoT sensor telemetry stream
    val inputDF = spark.readStream
      .format("rate")
      .option("rowsPerSecond", 10)
      .load()

    val eventsDF = inputDF
      .withColumn("sensor_id", concat(lit("sensor_"), ($"value" % 3)))
      .withColumn("event_time", expr("timestamp - interval 5 seconds"))
      .select($"sensor_id", $"event_time", $"value".as("reading"))

    println("=== Step 2: Applying Watermark and Windowed Stateful Aggregation ===")
    
    // Watermark of 10 minutes: allows data up to 10 minutes late and cleans up old state partitions
    val windowedAggDF = eventsDF
      .withWatermark("event_time", "10 minutes")
      .groupBy(
        window($"event_time", "5 minutes", "1 minutes"),
        $"sensor_id"
      )
      .agg(
        count("reading").as("total_readings"),
        avg("reading").as("avg_reading")
      )
      .select(
        $"sensor_id",
        $"window.start".as("window_start"),
        $"window.end".as("window_end"),
        $"total_readings",
        $"avg_reading"
      )

    println("=== Step 3: Launching Stateful Streaming Query with RocksDB Backend ===")
    
    val query = windowedAggDF.writeStream
      .format("console")
      .outputMode(OutputMode.Update()) // Update mode shows evolving window states as they aggregate
      .trigger(Trigger.ProcessingTime("2 seconds"))
      .queryName("Stateful_Watermark_Query")
      .start()

    // Let the streaming query run for 6 seconds to process batches, then terminate gracefully
    Thread.sleep(6000)
    println("\n=== Step 4: Stopping Stateful Streaming Query Gracefully ===")
    query.stop()
    query.awaitTermination(3000)

    spark.stop()
  }
}