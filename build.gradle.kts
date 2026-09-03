plugins {
    scala
    application
}

group = "com.shrikant.spark"
version = "1.0.0"

repositories {
    mavenCentral()
}

val scalaVersion: String by project
val sparkVersion: String by project

dependencies {
    // Scala
    implementation("org.scala-lang:scala-library:$scalaVersion")

    // Apache Spark
    implementation("org.apache.spark:spark-core_2.12:$sparkVersion")
    implementation("org.apache.spark:spark-sql_2.12:$sparkVersion")

    // Typesafe configuration
    implementation("com.typesafe:config:1.4.3")

    // Logging
    implementation("org.apache.logging.log4j:log4j-api:2.24.3")
    implementation("org.apache.logging.log4j:log4j-core:2.24.3")

    // Testing
    testImplementation("junit:junit:4.13.2")
}

application {
    mainClass.set("com.shrikant.spark.Main")
}

tasks.withType<ScalaCompile> {
    scalaCompileOptions.additionalParameters = listOf(
        "-deprecation",
        "-feature",
        "-unchecked"
    )
}

tasks.register<JavaExec>("runSparkFoundationScala") {
    group = "spark"
    description = "Run Module 1.2 Spark Foundation - Scala"

    classpath = sourceSets["main"].runtimeClasspath
    mainClass.set("com.shrikant.spark.foundation.SparkFoundation")

    jvmArgs(
        "--add-opens=java.base/java.lang=ALL-UNNAMED",
        "--add-opens=java.base/java.nio=ALL-UNNAMED",
        "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED"
    )
}

tasks.register<JavaExec>("runSparkFoundationJava") {
    group = "spark"
    description = "Run Module 1.2 Spark Foundation - Java"

    classpath = sourceSets["main"].runtimeClasspath
    mainClass.set("com.shrikant.sparkjava.foundation.SparkFoundationJava")

    jvmArgs(
        "--add-opens=java.base/java.lang=ALL-UNNAMED",
        "--add-opens=java.base/java.nio=ALL-UNNAMED",
        "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED"
    )
}

tasks.register<JavaExec>("runSparkFoundationSQL") {
    group = "spark"
    description = "Run Module 1.2 Spark Foundation - SQL"

    classpath = sourceSets["main"].runtimeClasspath
    mainClass.set("com.shrikant.spark.foundation.SparkFoundationSQL")

    jvmArgs(
        "--add-opens=java.base/java.lang=ALL-UNNAMED",
        "--add-opens=java.base/java.nio=ALL-UNNAMED",
        "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED"
    )
}

tasks.withType<Test> {
    useJUnit()

    jvmArgs(
        "--add-opens=java.base/java.lang=ALL-UNNAMED",
        "--add-opens=java.base/java.nio=ALL-UNNAMED",
        "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED"
    )
}

tasks.register<JavaExec>("runSparkFoundationPerformance") {
    group = "spark"
    description = "Run Module 1.2 Spark physical-plan performance investigation"

    classpath = sourceSets["main"].runtimeClasspath
    mainClass.set("com.shrikant.spark.foundation.SparkFoundationPerformance")

    jvmArgs(
        "--add-opens=java.base/java.lang=ALL-UNNAMED",
        "--add-opens=java.base/java.nio=ALL-UNNAMED",
        "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED"
    )
}

tasks.register<JavaExec>("runSparkFoundationBenchmark") {
    group = "spark"
    description = "Run Module 1.2 10-million-row Spark performance benchmark"

    classpath = sourceSets["main"].runtimeClasspath
    mainClass.set("com.shrikant.spark.foundation.SparkFoundationBenchmark")

    standardInput = System.`in`

    jvmArgs(
        "--add-opens=java.base/java.lang=ALL-UNNAMED",
        "--add-opens=java.base/java.nio=ALL-UNNAMED",
        "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED"
    )
}