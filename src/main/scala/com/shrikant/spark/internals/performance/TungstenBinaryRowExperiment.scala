package com.shrikant.spark.internals.performance

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Encoders
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder

/**
 * Module 1.9.2: Tungsten Binary Row Format & Specialized Encoders
 *
 * Demonstrates how Spark's Catalyst Encoders serialize domain objects into
 * compact binary structures optimized for the Tungsten execution engine.
 *
 * Usage:
 *   ./gradlew runTungstenBinaryRow
 */
object TungstenBinaryRowExperiment {

  case class UserRecord(id: Long, name: String, score: Double)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("TungstenBinaryRowExperiment")
      .master("local[2]")
      .config("spark.sql.shuffle.partitions", "2")
      .getOrCreate()

    import spark.implicits._

    println("==================================================")
    println("Executing Module 1.9.2: Tungsten Binary Row Format")
    println("==================================================")

    // 1. Create a Catalyst ExpressionEncoder using standard Spark Encoders product API
    val encoder: ExpressionEncoder[UserRecord] = Encoders.product[UserRecord].asInstanceOf[ExpressionEncoder[UserRecord]]

    val sampleRecord = UserRecord(42L, "Apache Spark", 99.5)
    println(s"--> Domain Object Case Class: $sampleRecord")
    println(s"--> Encoder Class Type: ${encoder.getClass.getName}")
    println(s"--> Encoder Schema Described: ${encoder.schema}")

    // 2. Demonstrate Dataset conversion and physical plan generation with Tungsten Encoders
    val df = Seq(
      UserRecord(1L, "Alice", 85.0),
      UserRecord(2L, "Bob", 92.0),
      UserRecord(3L, "Charlie", 78.5)
    ).toDF()

    println("\n--> Inspecting Catalyst Logical Plan & Physical Plan Encoders:")
    df.queryExecution.analyzed.foreach { plan =>
      println(s"Analyzed Plan Node: ${plan.nodeName}")
    }

    println("\n--> Physical Execution Plan (Showing WholeStageCodegen & Tungsten Row Processing):")
    df.show()
    df.explain(true)

    println("==================================================")
    println("Module 1.9.2 Binary Row Inspection Completed Successfully.")
    println("==================================================")

    spark.stop()
  }
}