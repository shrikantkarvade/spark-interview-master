package streaming

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{OutputMode, Trigger}

/**
 * Module 4.5: Stream-to-Stream Joins & Advanced Stateful Architecture
 * 
 * Demonstrates:
 * 1. Stream-to-stream inner join with dual watermarking and temporal constraints.
 * 2. Managing state buffering requirements for delayed stream records.
 */
object StreamJoinStatefulExperiment {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Module4_5_StreamJoinsAndState")
      .master("local[2]")
      .config("spark.sql.shuffle.partitions", "2")
      .getOrCreate()

    import spark.implicits._

    println("=== Step 1: Defining ClickStream and Impression Streams ===")
    
    // Simulate Impression Stream
    val impressions = spark.readStream
      .format("rate")
      .option("rowsPerSecond", 5)
      .load()
      .withColumn("imp_id", $"value")
      .withColumn("imp_time", $"timestamp")
      .select($"imp_id", $"imp_time")

    // Simulate Click Stream (arriving slightly later or concurrently)
    val clicks = spark.readStream
      .format("rate")
      .option("rowsPerSecond", 5)
      .load()
      .withColumn("click_id", $"value")
      .withColumn("click_time", $"timestamp")
      .select($"click_id", $"click_time")

    println("=== Step 2: Applying Watermarks and Temporal Stream-to-Stream Join ===")
    
    // Both streams MUST have watermarks defined for stream-to-stream joins
    val watermarkedImp = impressions.withWatermark("imp_time", "2 minutes")
    val watermarkedClick = clicks.withWatermark("click_time", "2 minutes")

    // Inner join with a time condition (clicks must happen within 1 minute after impression)
    val joinedDF = watermarkedImp
      .join(
        watermarkedClick,
        expr(
          """
          click_id = imp_id AND 
          click_time >= imp_time AND 
          click_time <= imp_time + interval 1 minute
          """
        ),
        joinType = "inner"
      )

    println("=== Step 3: Launching Joined Streaming Query ===")
    
    val query = joinedDF.writeStream
      .format("console")
      .outputMode(OutputMode.Append())
      .trigger(Trigger.ProcessingTime("2 seconds"))
      .queryName("Stream_To_Stream_Join_Query")
      .start()

    // Run for 6 seconds
    Thread.sleep(6000)

    println("\n=== Step 4: Stopping Query Gracefully ===")
    query.stop()
    query.awaitTermination(2000)

    spark.stop()
  }
}