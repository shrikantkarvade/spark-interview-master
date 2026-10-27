package com.shrikant.spark.lakehouse

import org.apache.spark.sql.SparkSession

object SparkSessionFactory {

  def createSession(appName: String): SparkSession = {
    val builder = SparkSession.builder()
      .appName(appName)
      // Delta Lake Catalog Extensions
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

      // Dynamic Allocation Defaults (if not overridden by spark-submit)
      .config("spark.dynamicAllocation.enabled", "true")
      .config("spark.dynamicAllocation.shuffle.tracking.enabled", "true")
      .config("spark.dynamicAllocation.minExecutors", "1")
      .config("spark.dynamicAllocation.maxExecutors", "10")
      .config("spark.dynamicAllocation.initialExecutors", "2")
      .config("spark.dynamicAllocation.executorIdleTimeout", "60s")

      // Kubernetes Memory Overhead Safeguard for OOM Prevention
      .config("spark.kubernetes.memoryOverheadFactor", "0.2")

    // Check if running locally vs cluster
    if (sys.env.get("SPARK_LOCAL_MODE").contains("true")) {
      builder.master("local[*]")
    }

    builder.getOrCreate()
  }
}