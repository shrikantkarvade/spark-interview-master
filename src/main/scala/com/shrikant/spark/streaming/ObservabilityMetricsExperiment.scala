package streaming

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{OutputMode, StreamingQueryListener, Trigger}

/**
 * Module 4.4: Production Observability & Advanced Metrics
 * 
 * Demonstrates:
 * 1. Extracting granular latency breakdowns (walCommit, addBatch, queryPlanning).
 * 2. Programmatically inspecting query status and recent progress snapshots.
 */
object ObservabilityMetricsExperiment {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Module4_4_ObservabilityAndMetrics")
      .master("local[2]")
      .config("spark.sql.shuffle.partitions", "2")
      .getOrCreate()

    import spark.implicits._

    println("=== Step 1: Registering Advanced Diagnostic Listener ===")
    
    spark.streams.addListener(new StreamingQueryListener {
      override def onQueryStarted(event: StreamingQueryListener.QueryStartedEvent): Unit = {
        println(s"\n[DIAGNOSTIC] Query Started -> Name: ${event.name}, ID: ${event.id}")
      }

      override def onQueryProgress(event: StreamingQueryListener.QueryProgressEvent): Unit = {
        val p = event.progress
        println(s"\n=== [METRICS REPORT] Batch: ${p.batchId} ===")
        println(s"  Timestamp         : ${p.timestamp}")
        println(s"  Rows Processed    : ${p.numInputRows}")
        println(f" Processing Rate : ${p.processedRowsPerSecond}%.2f rows/sec")
        
        // Granular latency breakdown (Crucial for bottleneck identification)
        val durations = p.durationMs
        println(s"  Latency Breakdown:")
        println(s"    - Trigger Execution : ${durations.get("triggerExecution")} ms")
        println(s"    - State Add Batch   : ${durations.get("addBatch")} ms")
        println(s"    - WAL Commit        : ${durations.get("walCommit")} ms")
        println(s"    - Offset Commit     : ${durations.get("commitOffsets")} ms")
      }

      override def onQueryTerminated(event: StreamingQueryListener.QueryTerminatedEvent): Unit = {
        println(s"\n[DIAGNOSTIC] Query Terminated -> ID: ${event.id}")
        event.exception.foreach(ex => println(s"  Error: $ex"))
      }
    })

    println("=== Step 2: Launching Monitored Streaming Query ===")
    
    val inputDF = spark.readStream
      .format("rate")
      .option("rowsPerSecond", 8)
      .load()

    val query = inputDF
      .withColumn("processed_val", $"value" * 10)
      .writeStream
      .format("console")
      .outputMode(OutputMode.Append())
      .trigger(Trigger.ProcessingTime("2 seconds"))
      .queryName("Monitored_Metrics_Query")
      .start()

    // Run for 6 seconds to capture a few micro-batches
    Thread.sleep(6000)

    println("\n=== Step 3: Inspecting Active Query Status Programmatically ===")
    val activeQuery = spark.streams.get(query.id)
    println(s"Query Name    : ${activeQuery.name}")
    println(s"Is Active     : ${activeQuery.isActive}")
    println(s"Last Progress : ${activeQuery.lastProgress}")

    println("\n=== Step 4: Stopping Query Gracefully ===")
    query.stop()
    query.awaitTermination(2000)

    spark.stop()
  }
}