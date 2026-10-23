package streaming

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{OutputMode, StreamingQueryListener, Trigger}
import java.io.File

object StreamingArchitectureExperiment {

  def main(args: Array[String]): Unit = {
    // 1. Force Hadoop user group information to use simple (non-secure) authentication
    // to bypass both Windows NativeIO and Linux UnixLoginModule checks.
    System.setProperty("HADOOP_USER_NAME", "root")

    // Point hadoop.home.dir to a valid local temp path so Hadoop doesn't throw a FileNotFound warning
    val winDir = new File(System.getProperty("java.io.tmpdir"), "hadoop-winutils")
    val binDir = new File(winDir, "bin")
    if (!binDir.exists()) binDir.mkdirs()
    System.setProperty("hadoop.home.dir", winDir.getAbsolutePath)

    val spark = SparkSession.builder()
      .appName("Module4_1_StreamingArchitecture")
      .master("local[2]")
      .config("spark.sql.shuffle.partitions", "2")
      .getOrCreate()

    // Disable file permission checks at the Hadoop Configuration level
    spark.sparkContext.hadoopConfiguration.set("fs.permissions.umask-mode", "000")
    spark.sparkContext.hadoopConfiguration.set("hadoop.security.authentication", "simple")

    import spark.implicits._

    println("=== Step 1: Registering Custom Telemetry StreamingQueryListener ===")

    spark.streams.addListener(new StreamingQueryListener {
      override def onQueryStarted(event: StreamingQueryListener.QueryStartedEvent): Unit = {
        println(s"[METRICS] Query Started: ${event.name} (ID: ${event.id})")
      }

      override def onQueryProgress(event: StreamingQueryListener.QueryProgressEvent): Unit = {
        val progress = event.progress
        println(s"\n--- [METRICS PROGRESS] Batch ID: ${progress.batchId} ---")
        println(s"  Input Rows Processed : ${progress.numInputRows}")
        println(s"  Input Rate           : ${progress.inputRowsPerSecond} rows/sec")
        println(s"  Processing Rate      : ${progress.processedRowsPerSecond} rows/sec")
        if (progress.durationMs.containsKey("triggerExecution")) {
          println(s"  Trigger Execution MS : ${progress.durationMs.get("triggerExecution")}")
        }
      }

      override def onQueryTerminated(event: StreamingQueryListener.QueryTerminatedEvent): Unit = {
        println(s"[METRICS] Query Terminated: ${event.id}")
        event.exception.foreach(ex => println(s"[METRICS ERROR] Cause: $ex"))
      }
    })

    println("\n=== Step 2: Launching Streaming Query using Rate Source & Console Sink ===")

    val streamingDF = spark.readStream
      .format("rate")
      .option("rowsPerSecond", 5)
      .option("rampUpTime", "1s")
      .load()

    val transformedDF = streamingDF
      .withColumn("value_doubled", $"value" * 2)
      .filter($"value" < 10L)

    val query = transformedDF.writeStream
      .format("console")
      .outputMode(OutputMode.Append())
      .trigger(Trigger.ProcessingTime("1 second"))
      .queryName("Architecture_Rate_Query")
      .start()

    // Let it run for 5 seconds to process batches, then stop gracefully
    Thread.sleep(5000)
    println("\n=== Step 3: Stopping Streaming Query Gracefully ===")
    query.stop()
    query.awaitTermination(3000)

    spark.stop()
  }
}