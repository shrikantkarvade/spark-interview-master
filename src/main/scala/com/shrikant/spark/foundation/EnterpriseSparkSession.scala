package com.shrikant.spark.foundation

import org.apache.spark.sql.SparkSession

object EnterpriseSparkSession {

  def create(
              appName: String,
              environment: String
            ): SparkSession = {

    val config =
      SparkApplicationConfig.load(environment)

    SparkSession.builder()
      .appName(appName)
      .master(config.master)
      .config(
        "spark.sql.shuffle.partitions",
        config.shufflePartitions
      )
      .config(
        "spark.sql.adaptive.enabled",
        config.adaptiveExecutionEnabled
      )
      .getOrCreate()
  }
}
