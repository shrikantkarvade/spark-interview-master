package com.shrikant.spark.foundation

import com.typesafe.config.ConfigFactory
import org.junit.Test
import org.junit.Assert._

class SparkApplicationConfigTest {

  @Test
  def shouldLoadDevConfiguration(): Unit = {

    val config =
      SparkApplicationConfig.load("dev")

    assertEquals("dev", config.environment)
    assertEquals("local[2]", config.master)
    assertEquals(20, config.shufflePartitions)
    assertTrue(config.adaptiveExecutionEnabled)
  }

  @Test
  def shouldLoadTestConfiguration(): Unit = {

    val config =
      SparkApplicationConfig.load("test")

    assertEquals("test", config.environment)
    assertEquals("local[2]", config.master)
    assertEquals(50, config.shufflePartitions)
    assertTrue(config.adaptiveExecutionEnabled)
  }

  @Test
  def shouldLoadProdConfiguration(): Unit = {

    val config =
      SparkApplicationConfig.load("prod")

    assertEquals("prod", config.environment)
    assertEquals("local[2]", config.master)
    assertEquals(400, config.shufflePartitions)
    assertTrue(config.adaptiveExecutionEnabled)
  }

  @Test
  def shouldNormalizeEnvironment(): Unit = {

    val config =
      SparkApplicationConfig.load(" TEST ")

    assertEquals("test", config.environment)
    assertEquals(50, config.shufflePartitions)
  }

  @Test
  def shouldNormalizeUppercaseEnvironment(): Unit = {

    val config =
      SparkApplicationConfig.load("PROD")

    assertEquals("prod", config.environment)
    assertEquals(400, config.shufflePartitions)
  }

  @Test
  def shouldRejectUnsupportedEnvironment(): Unit = {

    var exception: IllegalArgumentException = null

    try {
      SparkApplicationConfig.load("uat")
      fail("Expected IllegalArgumentException")
    } catch {
      case e: IllegalArgumentException =>
        exception = e
    }

    assertNotNull(exception)

    assertTrue(
      exception.getMessage.contains("Unsupported environment")
    )
  }

  @Test
  def shouldRejectEmptySparkMaster(): Unit = {

    val config =
      SparkApplicationConfig(
        environment = "test",
        master = "",
        shufflePartitions = 50,
        adaptiveExecutionEnabled = true
      )

    var exception: IllegalArgumentException = null

    try {
      SparkApplicationConfig.validate(config)
      fail("Expected IllegalArgumentException")
    } catch {
      case e: IllegalArgumentException =>
        exception = e
    }

    assertNotNull(exception)

    assertTrue(
      exception.getMessage.contains(
        "spark.master must not be empty"
      )
    )
  }

  @Test
  def shouldRejectZeroShufflePartitions(): Unit = {

    val config =
      SparkApplicationConfig(
        environment = "test",
        master = "local[2]",
        shufflePartitions = 0,
        adaptiveExecutionEnabled = true
      )

    var exception: IllegalArgumentException = null

    try {
      SparkApplicationConfig.validate(config)
      fail("Expected IllegalArgumentException")
    } catch {
      case e: IllegalArgumentException =>
        exception = e
    }

    assertNotNull(exception)

    assertTrue(
      exception.getMessage.contains(
        "spark.sql.shuffle.partitions must be greater than 0"
      )
    )
  }

  @Test
  def shouldRejectNegativeShufflePartitions(): Unit = {

    val config =
      SparkApplicationConfig(
        environment = "test",
        master = "local[2]",
        shufflePartitions = -1,
        adaptiveExecutionEnabled = true
      )

    var exception: IllegalArgumentException = null

    try {
      SparkApplicationConfig.validate(config)
      fail("Expected IllegalArgumentException")
    } catch {
      case e: IllegalArgumentException =>
        exception = e
    }

    assertNotNull(exception)

    assertTrue(
      exception.getMessage.contains(
        "spark.sql.shuffle.partitions must be greater than 0"
      )
    )
  }

  @Test
  def shouldRejectMissingSparkMaster(): Unit = {

    val config =
      ConfigFactory.parseString(
        """
          |spark {
          |  sql {
          |    shuffle.partitions = 50
          |    adaptive.enabled = true
          |  }
          |}
          |""".stripMargin
      )

    var exception: SparkApplicationConfigException = null

    try {
      SparkApplicationConfig.fromConfig(
        environment = "test",
        config = config
      )

      fail("Expected SparkApplicationConfigException")
    } catch {
      case e: SparkApplicationConfigException =>
        exception = e
    }

    assertNotNull(exception)
    assertNotNull(exception.getMessage)

    assertTrue(
      exception.getCause.isInstanceOf[com.typesafe.config.ConfigException]
    )
  }

  @Test
  def shouldRejectMissingShufflePartitions(): Unit = {

    val config =
      ConfigFactory.parseString(
        """
          |spark {
          |  master = "local[2]"
          |  sql {
          |    adaptive.enabled = true
          |  }
          |}
          |""".stripMargin
      )

    var exception: SparkApplicationConfigException = null

    try {
      SparkApplicationConfig.fromConfig(
        environment = "test",
        config = config
      )

      fail("Expected SparkApplicationConfigException")
    } catch {
      case e: SparkApplicationConfigException =>
        exception = e
    }

    assertNotNull(exception)
    assertNotNull(exception.getMessage)

    assertTrue(
      exception.getCause.isInstanceOf[com.typesafe.config.ConfigException]
    )
  }

  @Test
  def shouldRejectMissingAdaptiveExecutionSetting(): Unit = {

    val config =
      ConfigFactory.parseString(
        """
          |spark {
          |  master = "local[2]"
          |  sql {
          |    shuffle.partitions = 50
          |  }
          |}
          |""".stripMargin
      )

    var exception: SparkApplicationConfigException = null

    try {
      SparkApplicationConfig.fromConfig(
        environment = "test",
        config = config
      )

      fail("Expected SparkApplicationConfigException")
    } catch {
      case e: SparkApplicationConfigException =>
        exception = e
    }

    assertNotNull(exception)
    assertNotNull(exception.getMessage)

    assertTrue(
      exception.getCause.isInstanceOf[com.typesafe.config.ConfigException]
    )
  }

  @Test
  def shouldTranslateConfigurationFailure(): Unit = {

    val config =
      ConfigFactory.parseString(
        """
          |spark {
          |  master = "local[2]"
          |  sql {
          |    adaptive.enabled = true
          |  }
          |}
          |""".stripMargin
      )

    var exception: SparkApplicationConfigException = null

    try {
      SparkApplicationConfig.fromConfig(
        environment = "test",
        config = config
      )

      fail("Expected SparkApplicationConfigException")
    } catch {
      case e: SparkApplicationConfigException =>
        exception = e
    }

    assertNotNull(exception)
    assertNotNull(exception.getMessage)

    assertTrue(
      exception.getCause.isInstanceOf[com.typesafe.config.ConfigException]
    )
  }
}