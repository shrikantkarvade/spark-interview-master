package com.shrikant.spark.lakehouse

import org.apache.spark.sql.SparkSession
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.BeforeAndAfterAll
import java.nio.file.Files

class MedallionPipelineSpec extends AnyFunSuite with BeforeAndAfterAll {

  // Declared as a val so it acts as a stable identifier for imports
  val spark: SparkSession = SparkSession.builder()
    .appName("MedallionPipelineSpec")
    .master("local[2]")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.ui.enabled", "false")
    .getOrCreate()

  val tempDir: String = Files.createTempDirectory("delta-test-").toAbsolutePath.toString

  override def afterAll(): Unit = {
    if (spark != null) {
      spark.stop()
    }
  }

  test("Bronze layer should successfully ingest and write Delta table") {
    // Now valid because spark is a stable identifier (val)
    import spark.implicits._

    // 1. Create deterministic test data
    val testData = Seq(
      (1, "Alice", "Engineering", 75000, "2026-10-01"),
      (2, "Bob", "Marketing", 62000, "2026-10-02")
    ).toDF("id", "name", "department", "salary", "ingest_date")

    val bronzePath = s"$tempDir/delta-bronze"

    // 2. Write to Delta sink
    testData.write
      .format("delta")
      .mode("overwrite")
      .save(bronzePath)

    // 3. Read back and assert
    val readDf = spark.read.format("delta").load(bronzePath)

    assert(readDf.count() == 2)
    assert(readDf.filter($"department" === "Engineering").count() == 1)
  }
}