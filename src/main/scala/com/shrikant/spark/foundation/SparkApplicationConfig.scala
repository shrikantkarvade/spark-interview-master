package com.shrikant.spark.foundation

import com.typesafe.config.{Config, ConfigFactory}

final case class SparkApplicationConfig(
                                         environment: String,
                                         master: String,
                                         shufflePartitions: Int,
                                         adaptiveExecutionEnabled: Boolean
                                       )

object SparkApplicationConfig {

  private val supportedEnvironments =
    Set("dev", "test", "prod")

  private[foundation] def validate(
                                    config: SparkApplicationConfig
                                  ): Unit = {

    require(
      config.master.nonEmpty,
      "spark.master must not be empty"
    )

    require(
      config.shufflePartitions > 0,
      s"spark.sql.shuffle.partitions must be greater than 0, " +
        s"but was ${config.shufflePartitions}"
    )
  }

  def load(environment: String): SparkApplicationConfig = {

    val normalizedEnvironment =
      environment.trim.toLowerCase

    require(
      supportedEnvironments.contains(normalizedEnvironment),
      s"Unsupported environment: '$environment'. " +
        s"Supported environments: ${supportedEnvironments.toSeq.sorted.mkString(", ")}"
    )

    val configFile =
      s"config/application-$normalizedEnvironment.conf"

    val config =
      ConfigFactory.load(configFile)

    fromConfig(
      environment = normalizedEnvironment,
      config = config
    )
  }

  private[foundation] def fromConfig(
                                      environment: String,
                                      config: Config
                                    ): SparkApplicationConfig = {

    try {

      val applicationConfig =
        SparkApplicationConfig(
          environment = environment,
          master = config.getString("spark.master"),
          shufflePartitions =
            config.getInt("spark.sql.shuffle.partitions"),
          adaptiveExecutionEnabled =
            config.getBoolean("spark.sql.adaptive.enabled")
        )

      validate(applicationConfig)

      applicationConfig

    } catch {
      case e: com.typesafe.config.ConfigException =>
        throw new SparkApplicationConfigException(
          s"Invalid Spark application configuration " +
            s"for environment '$environment'",
          e
        )
    }
  }
}