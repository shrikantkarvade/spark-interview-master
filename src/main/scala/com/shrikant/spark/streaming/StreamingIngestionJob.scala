package com.shrikant.spark.streaming

import org.apache.spark.sql.{DataFrame, Encoders, SparkSession}
import org.apache.spark.sql.streaming.{OutputMode, StreamingQuery, Trigger}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object StreamingIngestionJob {

  // Case class for typed transformations
  case class RawSensorEvent(
                             eventId: String,
                             eventTimestamp: String,
                             deviceId: String,
                             metricValue: Double
                           )

  case class ProcessedSensorEvent(
                                   eventId: String,
                                   eventTimestamp: java.sql.Timestamp,
                                   deviceId: String,
                                   metricValue: Double,
                                   ingestedAt: java.sql.Timestamp
                                 )

  /** Applies watermarking, deduplication, and standard schema enrichment on a streaming DataFrame.
   */
  def transformStream(inputDF: DataFrame): DataFrame = {
    // Bring spark implicits into scope for $ string interpolation
    val spark = inputDF.sparkSession
    import spark.implicits._

    inputDF
      .withColumn("eventTimestamp", $"eventTimestamp".cast(TimestampType))
      .filter($"eventId".isNotNull && $"eventTimestamp".isNotNull)
      .withWatermark("eventTimestamp", "10 minutes")
      .dropDuplicates("eventId", "eventTimestamp")
      .withColumn("ingestedAt", current_timestamp())
  }

  /** Computes stateful windowed aggregations (5-minute window, sliding every 1 minute)
   * with watermarking for late-data management.
   */
  def aggregateSensorStream(processedDF: DataFrame): DataFrame = {
    val spark = processedDF.sparkSession
    import spark.implicits._

    processedDF
      .groupBy(
        window($"eventTimestamp", "5 minutes", "1 minute"),
        $"deviceId"
      )
      .agg(
        avg("metricValue").as("avgMetricValue"),
        max("metricValue").as("maxMetricValue"),
        count("eventId").as("eventCount")
      )
      .select(
        $"window.start".as("windowStart"),
        $"window.end".as("windowEnd"),
        $"deviceId",
        $"avgMetricValue",
        $"maxMetricValue",
        $"eventCount"
      )
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("StreamingLakehouseIngestion")
      .master("local[*]")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog"
      )
      // RocksDB State Store Provider for out-of-core stateful stream processing
      .config(
        "spark.sql.streaming.stateStore.providerClass",
        "org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider"
      )
      .getOrCreate()

    val inputPath = args.headOption.getOrElse("./data/streaming/input")
    val checkpointPath =
      args.lift(1).getOrElse("./data/streaming/checkpoints/silver_events")
    val deltaSinkPath =
      args.lift(2).getOrElse("./data/delta/silver_sensor_events")

    // Derive schema safely via Encoders
    val schema = Encoders.product[RawSensorEvent].schema

    val rawStreamDF = spark.readStream
      .schema(schema)
      .json(inputPath)

    val processedDF = transformStream(rawStreamDF)

    val query: StreamingQuery = processedDF.writeStream
      .format("delta")
      .outputMode(OutputMode.Append())
      .option("checkpointLocation", checkpointPath)
      .trigger(Trigger.ProcessingTime("10 seconds"))
      .start(deltaSinkPath)

    query.awaitTermination()

    val aggregatedDF = aggregateSensorStream(processedDF)

    val aggregateQuery: StreamingQuery = aggregatedDF.writeStream
      .format("delta")
      .outputMode(OutputMode.Update()) // Update mode for stateful aggregations
      .option("checkpointLocation", checkpointPath + "_agg")
      .trigger(Trigger.ProcessingTime("10 seconds"))
      .start(deltaSinkPath + "_gold_aggregates")
  }
}