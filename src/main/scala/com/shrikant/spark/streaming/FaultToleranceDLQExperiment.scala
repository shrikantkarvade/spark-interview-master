package streaming

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.streaming.{OutputMode, Trigger}

/**
 * Module 4.3: Fault Tolerance, Exactly-Once Semantics & DLQ Patterns
 * 
 * Demonstrates:
 * 1. Handling schema corruption gracefully using PERMISSIVE mode and `_corrupt_record`.
 * 2. Implementing a Dead Letter Queue (DLQ) pattern to isolate poisoned payloads.
 * 3. Maintaining pipeline uptime despite incoming malformed streaming data.
 */
object FaultToleranceDLQExperiment {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Module4_3_FaultToleranceAndDLQ")
      .master("local[2]")
      .config("spark.sql.shuffle.partitions", "2")
      .getOrCreate()

    import spark.implicits._

    println("=== Step 1: Simulating Stream with Valid & Malformed JSON Records ===")
    
    // Generate an incoming rate stream and inject intentional JSON corruption on odd values
    val inputDF = spark.readStream
      .format("rate")
      .option("rowsPerSecond", 5)
      .load()

    val jsonStreamDF = inputDF
      .withColumn("json_data", 
        when($"value" % 2 === 0, lit(concat(lit("{\"id\": "), $"value", lit(", \"status\": \"SUCCESS\"}"))))
        .otherwise(lit(concat(lit("{malformed_json_key: "), $"value", lit("}")))) // Intentional syntax error
      )
      .select("json_data")

    println("=== Step 2: Defining Schema with Corrupt Record Column (DLQ Capture) ===")
    
    val schema = new StructType()
      .add("id", LongType)
      .add("status", StringType)
      .add("_corrupt_record", StringType) // Spark stores unparseable rows here under PERMISSIVE mode

    val parsedDF = jsonStreamDF
      .select(from_json($"json_data", schema, Map("mode" -> "PERMISSIVE")) as "data")
      .select("data.*")

    println("=== Step 3: Splitting into Main Processing Pipeline & Dead Letter Queue (DLQ) ===")
    
    // Main clean pipeline
    val cleanDF = parsedDF.filter($"_corrupt_record".isNull).drop("_corrupt_record")
    
    // DLQ pipeline for poisoned payloads
    val dlqDF = parsedDF.filter($"_corrupt_record".isNotNull).select($"_corrupt_record".as("poisoned_payload"))

    println("=== Step 4: Launching Dual Streaming Sinks ===")
    
    val cleanQuery = cleanDF.writeStream
      .format("console")
      .outputMode(OutputMode.Append())
      .trigger(Trigger.ProcessingTime("2 seconds"))
      .queryName("Clean_Records_Query")
      .start()

    val dlqQuery = dlqDF.writeStream
      .format("console")
      .outputMode(OutputMode.Append())
      .trigger(Trigger.ProcessingTime("2 seconds"))
      .queryName("DLQ_Poison_Pill_Query")
      .start()

    // Let the streaming queries run for 6 seconds to process batches
    Thread.sleep(6000)

    println("\n=== Step 5: Stopping Queries Gracefully ===")
    cleanQuery.stop()
    dlqQuery.stop()
    cleanQuery.awaitTermination(2000)
    dlqQuery.awaitTermination(2000)

    spark.stop()
  }
}