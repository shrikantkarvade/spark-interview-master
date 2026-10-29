package com.shrikant.spark.streaming

import io.delta.tables.DeltaTable
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.streaming.OutputMode
import org.scalatest.BeforeAndAfter
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.io.File
import java.nio.file.Files

class StreamingPipelineSpec extends AnyFunSuite with Matchers with BeforeAndAfter {

  private var spark: SparkSession = _
  private var tempDir: File = _

  before {
    tempDir = Files.createTempDirectory("spark_streaming_test_").toFile

    spark = SparkSession
      .builder()
      .appName("StreamingPipelineSpec")
      .master("local[2]")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog"
      )
      .config("spark.sql.shuffle.partitions", "2")
      .getOrCreate()
  }

  after {
    if (spark != null) {
      spark.stop()
    }
    if (tempDir != null && tempDir.exists()) {
      deleteRecursively(tempDir)
    }
  }

  private def deleteRecursively(file: File): Unit = {
    if (file.isDirectory) {
      val files = file.listFiles()
      if (files != null) files.foreach(deleteRecursively)
    }
    file.delete()
  }

  test("Streaming pipeline correctly deduplicates events and writes to Delta Lake sink") {
    val stableSpark = spark
    import stableSpark.implicits._

    implicit val sqlContext = spark.sqlContext

    val checkpointLocation = new File(tempDir, "checkpoints").getAbsolutePath
    val deltaPath = new File(tempDir, "delta_silver").getAbsolutePath

    // MemoryStream lets us inject batch data programmatically
    val inputStream = MemoryStream[StreamingIngestionJob.RawSensorEvent]

    val inputDF = inputStream.toDS().toDF()
    val transformedDF = StreamingIngestionJob.transformStream(inputDF)

    // Start streaming query using availableNow/default micro-batch trigger
    val query = transformedDF.writeStream
      .format("delta")
      .outputMode(OutputMode.Append())
      .option("checkpointLocation", checkpointLocation)
      .start(deltaPath)

    // Batch 1: Push initial events (including duplicate event ID 'evt-101')
    val batch1 = Seq(
      StreamingIngestionJob.RawSensorEvent("evt-101", "2026-10-29T10:00:00Z", "sensor-A", 42.5),
      StreamingIngestionJob.RawSensorEvent("evt-101", "2026-10-29T10:00:00Z", "sensor-A", 42.5), // Duplicate
      StreamingIngestionJob.RawSensorEvent("evt-102", "2026-10-29T10:05:00Z", "sensor-B", 88.1)
    )

    inputStream.addData(batch1)
    query.processAllAvailable()

    // Assert Batch 1 in Delta Table
    val deltaTable = DeltaTable.forPath(spark, deltaPath)
    val resultBatch1 = deltaTable.toDF

    resultBatch1.count() shouldBe 2
    resultBatch1.filter($"eventId" === "evt-101").count() shouldBe 1

    // Batch 2: Push new event
    val batch2 = Seq(
      StreamingIngestionJob.RawSensorEvent("evt-103", "2026-10-29T10:08:00Z", "sensor-A", 50.0)
    )

    inputStream.addData(batch2)
    query.processAllAvailable()

    // Assert final combined records
    deltaTable.toDF.count() shouldBe 3

    query.stop()
  }

  test("Streaming pipeline correctly computes stateful windowed aggregations") {
    val stableSpark = spark
    import stableSpark.implicits._

    implicit val sqlContext = spark.sqlContext

    val checkpointLocation = new File(tempDir, "checkpoints_agg").getAbsolutePath
    val deltaPath = new File(tempDir, "delta_gold_agg").getAbsolutePath

    val inputStream = MemoryStream[StreamingIngestionJob.RawSensorEvent]
    val inputDF = inputStream.toDS().toDF()

    val transformedDF = StreamingIngestionJob.transformStream(inputDF)
    val aggregatedDF = StreamingIngestionJob.aggregateSensorStream(transformedDF)

    val query = aggregatedDF.writeStream
      .format("delta")
      .outputMode(OutputMode.Update())
      .option("checkpointLocation", checkpointLocation)
      .start(deltaPath)

    // Push events falling into the same event-time window (10:00 - 10:05)
    val batch = Seq(
      StreamingIngestionJob.RawSensorEvent("evt-201", "2026-10-29T10:01:00Z", "sensor-X", 10.0),
      StreamingIngestionJob.RawSensorEvent("evt-202", "2026-10-29T10:03:00Z", "sensor-X", 30.0)
    )

    inputStream.addData(batch)
    query.processAllAvailable()

    val deltaTable = DeltaTable.forPath(spark, deltaPath)
    val resultDF = deltaTable.toDF

    resultDF.count() shouldBe >(0L)
    val sensorXRow = resultDF.filter($"deviceId" === "sensor-X").head()
    sensorXRow.getAs[Double]("avgMetricValue") shouldBe 20.0
    sensorXRow.getAs[Long]("eventCount") shouldBe 2L

    query.stop()
  }
}