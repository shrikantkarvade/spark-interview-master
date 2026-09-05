package com.shrikant.spark.foundation

object EnterpriseSparkSessionDemo {

  def main(args: Array[String]): Unit = {

    println()
    println("====================================================")
    println("MODULE 1.3 - ENTERPRISE SPARKSESSION")
    println("====================================================")

    val environment =
      args.headOption.getOrElse("dev")

    println()
    println("APPLICATION")
    println("====================================================")

    println(s"Environment : $environment")

    val spark =
      EnterpriseSparkSession.create(
        appName = s"Module 1.3 - Enterprise - $environment",
        environment = environment
      )

    // ====================================================
    // SESSION CONFIGURATION ISOLATION
    // ====================================================

    val session1 = spark

    val session2 =
      spark.newSession()

    println()
    println("SESSION CONFIGURATION ISOLATION")
    println("====================================================")

    println(
      s"Session 1 before override : " +
        s"${session1.conf.get("spark.sql.shuffle.partitions")}"
    )

    println(
      s"Session 2 before override : " +
        s"${session2.conf.get("spark.sql.shuffle.partitions")}"
    )

    session1.conf.set(
      "spark.sql.shuffle.partitions",
      10
    )

    println(
      s"Session 1 after override  : " +
        s"${session1.conf.get("spark.sql.shuffle.partitions")}"
    )

    println(
      s"Session 2 after override  : " +
        s"${session2.conf.get("spark.sql.shuffle.partitions")}"
    )

    // ====================================================
    // EFFECTIVE SPARK CONFIGURATION
    // ====================================================

    println()
    println("EFFECTIVE SPARK CONFIGURATION")
    println("====================================================")

    println(
      s"spark.sql.shuffle.partitions = " +
        s"${spark.conf.get("spark.sql.shuffle.partitions")}"
    )

    println(
      s"spark.sql.adaptive.enabled   = " +
        s"${spark.conf.get("spark.sql.adaptive.enabled")}"
    )

    // ====================================================
    // SPARK APPLICATION
    // ====================================================

    println()
    println("SPARK APPLICATION")
    println("====================================================")

    println(
      s"Application : ${spark.sparkContext.appName}"
    )

    println(
      s"Master      : ${spark.sparkContext.master}"
    )

    // ====================================================
    // QUERY
    // ====================================================

    println()
    println("QUERY")
    println("====================================================")

    println(
      s"Count : ${spark.range(100).count()}"
    )

    // ====================================================
    // SESSION LIFECYCLE EXPERIMENT
    // ====================================================

    println()
    println("SESSION LIFECYCLE EXPERIMENT")
    println("====================================================")

    val session3 =
      spark.newSession()

    println(
      s"Shared SparkContext : " +
        s"${session3.sparkContext eq spark.sparkContext}"
    )

    println(
      s"Session 1 count before session3.stop : " +
        s"${spark.range(10).count()}"
    )

    session3.stop()

    println("Session 3 stopped.")

    try {
      println(
        s"Session 1 count after session3.stop  : " +
          s"${spark.range(10).count()}"
      )
    } catch {
      case e: IllegalStateException =>
        println(
          s"Session 1 unusable after session3.stop : " +
            s"${e.getMessage.split("\n").head}"
        )
    }

    // ====================================================
    // SHUTDOWN
    // ====================================================

    println()
    println("SHUTDOWN")
    println("====================================================")

    println(
      "SparkContext already stopped by session3.stop()."
    )
  }
}