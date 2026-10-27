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
val deltaLakeVersion: String by project

dependencies {
    // Scala
    implementation("org.scala-lang:scala-library:$scalaVersion")

    // Apache Spark
    implementation("org.apache.spark:spark-core_2.12:$sparkVersion")
    implementation("org.apache.spark:spark-sql_2.12:$sparkVersion")

    // Typesafe configuration
    implementation("com.typesafe:config:1.4.3")

    // Logging
    implementation("org.apache.logging.log4j:log4j-api:2.26.1")
    implementation("org.apache.logging.log4j:log4j-core:2.26.1")

    // Delta Lake Spark
    implementation("io.delta:delta-spark_2.12:$deltaLakeVersion")

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

tasks.register<JavaExec>("runEnterpriseSparkSession") {
    group = "spark"
    description = "Run Module 1.3 Enterprise SparkSession demonstration"

    classpath = sourceSets["main"].runtimeClasspath
    mainClass.set("com.shrikant.spark.foundation.EnterpriseSparkSessionDemo")

    jvmArgs(
        "--add-opens=java.base/java.lang=ALL-UNNAMED",
        "--add-opens=java.base/java.nio=ALL-UNNAMED",
        "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED"
    )
}

tasks.register<JavaExec>("runLogicalPlanExperiment") {
    group = "spark"
    description = "Run Module 1.4 logical-to-physical plan experiment"

    classpath = sourceSets["main"].runtimeClasspath
    mainClass.set("com.shrikant.spark.planning.LogicalPlanExperiment")

    jvmArgs(
        "--add-opens=java.base/java.lang=ALL-UNNAMED",
        "--add-exports=java.base/sun.nio.ch=ALL-UNNAMED"
    )

    args = project.findProperty("args")
        ?.toString()
        ?.split(" ")
        ?: emptyList()
}

tasks.register<JavaExec>("runDataSourcePushdownExperiment") {
    group = "spark"
    description = "Run Module 1.4.11 data source filter and projection pushdown experiment"

    classpath = sourceSets["main"].runtimeClasspath
    mainClass.set("com.shrikant.spark.planning.DataSourcePushdownExperiment")

    jvmArgs(
        "--add-opens=java.base/java.lang=ALL-UNNAMED",
        "--add-exports=java.base/sun.nio.ch=ALL-UNNAMED"
    )

    args = project.findProperty("args")
        ?.toString()
        ?.split(" ")
        ?: emptyList()
}

tasks.register<JavaExec>("runParquetFixtureWriter") {
    group = "spark"
    description = "Generate Module 1.4.11 Parquet fixture"

    classpath = sourceSets["main"].runtimeClasspath
    mainClass.set("com.shrikant.spark.planning.ParquetFixtureWriter")

    jvmArgs(
        "--add-opens=java.base/java.lang=ALL-UNNAMED",
        "--add-exports=java.base/sun.nio.ch=ALL-UNNAMED"
    )
}

tasks.register<JavaExec>("runPartitionBaselineExperiment") {
    group = "spark"
    description = "Run Module 1.5.1 partition baseline experiment"

    classpath = sourceSets["main"].runtimeClasspath
    mainClass.set(
        "com.shrikant.spark.partitioning.PartitionBaselineExperiment"
    )

    jvmArgs(
        "--add-opens=java.base/java.lang=ALL-UNNAMED",
        "--add-exports=java.base/sun.nio.ch=ALL-UNNAMED"
    )
}

tasks.register<JavaExec>("runRepartitionExperiment") {
    group = "spark"
    description = "Run Module 1.5.3 repartition experiment"

    classpath = sourceSets["main"].runtimeClasspath
    mainClass.set(
        "com.shrikant.spark.partitioning.RepartitionExperiment"
    )

    jvmArgs(
        "--add-opens=java.base/java.lang=ALL-UNNAMED",
        "--add-exports=java.base/sun.nio.ch=ALL-UNNAMED"
    )
}

tasks.register<JavaExec>("runCoalesceExperiment") {
    group = "spark"
    description = "Run Module 1.5.4 coalesce experiment"

    classpath = sourceSets["main"].runtimeClasspath
    mainClass.set(
        "com.shrikant.spark.partitioning.CoalesceExperiment"
    )

    jvmArgs(
        "--add-opens=java.base/java.lang=ALL-UNNAMED",
        "--add-exports=java.base/sun.nio.ch=ALL-UNNAMED"
    )
}

tasks.register<JavaExec>("runPartitionChangeBenchmark") {
    group = "spark"
    description = "Run Module 1.5.5 repartition vs coalesce benchmark"

    classpath = sourceSets["main"].runtimeClasspath
    mainClass.set(
        "com.shrikant.spark.partitioning.PartitionChangeBenchmark"
    )

    jvmArgs(
        "--add-opens=java.base/java.lang=ALL-UNNAMED",
        "--add-exports=java.base/sun.nio.ch=ALL-UNNAMED"
    )
}

tasks.register<JavaExec>("runHashPartitioningExperiment") {
    group = "spark"
    description = "Run Module 1.5.6 hash partitioning experiment"

    classpath = sourceSets["main"].runtimeClasspath
    mainClass.set(
        "com.shrikant.spark.partitioning.HashPartitioningExperiment"
    )

    jvmArgs(
        "--add-opens=java.base/java.lang=ALL-UNNAMED",
        "--add-exports=java.base/sun.nio.ch=ALL-UNNAMED"
    )
}

tasks.register<JavaExec>("runPartitionImbalanceExperiment") {
    group = "spark"
    description = "Run Module 1.5.7 partition imbalance experiment"

    classpath = sourceSets["main"].runtimeClasspath
    mainClass.set(
        "com.shrikant.spark.partitioning.PartitionImbalanceExperiment"
    )

    jvmArgs(
        "--add-opens=java.base/java.lang=ALL-UNNAMED",
        "--add-exports=java.base/sun.nio.ch=ALL-UNNAMED"
    )
}

tasks.register<JavaExec>("runDataSkewMitigationExperiment") {
    group = "spark"
    description = "Run Module 1.5.8 data skew mitigation experiment"

    classpath = sourceSets["main"].runtimeClasspath
    mainClass.set(
        "com.shrikant.spark.partitioning.DataSkewMitigationExperiment"
    )

    jvmArgs(
        "--add-opens=java.base/java.lang=ALL-UNNAMED",
        "--add-exports=java.base/sun.nio.ch=ALL-UNNAMED"
    )
}

tasks.register<JavaExec>("runAqePartitionManagementExperiment") {
    group = "spark"
    description = "Run Module 1.5.9 AQE partition management experiment"

    classpath = sourceSets["main"].runtimeClasspath
    mainClass.set(
        "com.shrikant.spark.partitioning.AqePartitionManagementExperiment"
    )

    jvmArgs(
        "--add-opens=java.base/java.lang=ALL-UNNAMED",
        "--add-exports=java.base/sun.nio.ch=ALL-UNNAMED"
    )
}

tasks.register<JavaExec>("runProductionPartitionSizingExperiment") {
    group = "spark"
    description = "Run Module 1.5.10 production partition sizing experiment"

    classpath = sourceSets["main"].runtimeClasspath
    mainClass.set(
        "com.shrikant.spark.partitioning.ProductionPartitionSizingExperiment"
    )

    jvmArgs(
        "--add-opens=java.base/java.lang=ALL-UNNAMED",
        "--add-exports=java.base/sun.nio.ch=ALL-UNNAMED"
    )
}

tasks.register<JavaExec>("runJoinBaselineExperiment") {

    group = "spark"

    description =
        "Run Module 1.6.1 baseline join experiment"

    classpath =
        sourceSets["main"].runtimeClasspath

    mainClass.set(
        "com.shrikant.spark.joins.JoinBaselineExperiment"
    )

    jvmArgs(
        "--add-opens=java.base/java.lang=ALL-UNNAMED",
        "--add-exports=java.base/sun.nio.ch=ALL-UNNAMED"
    )
}

tasks.register<JavaExec>("runBroadcastHashJoinExperiment") {
    group = "spark"
    description = "Module 1.6.2 - Broadcast Hash Join experiment"

    classpath = sourceSets["main"].runtimeClasspath
    mainClass.set("com.shrikant.spark.joins.BroadcastHashJoinExperiment")

    jvmArgs(
        "--add-opens=java.base/java.lang=ALL-UNNAMED",
        "--add-exports=java.base/sun.nio.ch=ALL-UNNAMED"
    )
}

tasks.register<JavaExec>("runBroadcastThresholdExperiment") {
    group = "spark"
    description = "Module 1.6.3 - Broadcast Threshold experiment"

    classpath = sourceSets["main"].runtimeClasspath
    mainClass.set("com.shrikant.spark.joins.BroadcastThresholdExperiment")

    jvmArgs(
        "--add-opens=java.base/java.lang=ALL-UNNAMED",
        "--add-exports=java.base/sun.nio.ch=ALL-UNNAMED"
    )
}

tasks.register<JavaExec>("runSortMergeJoinExperiment") {
    group = "spark"
    description = "Module 1.6.4 - Sort Merge Join experiment"

    classpath = sourceSets["main"].runtimeClasspath
    mainClass.set("com.shrikant.spark.joins.SortMergeJoinExperiment")

    jvmArgs(
        "--add-opens=java.base/java.lang=ALL-UNNAMED",
        "--add-exports=java.base/sun.nio.ch=ALL-UNNAMED"
    )
}

tasks.register<JavaExec>("runShuffleHashJoinExperiment") {
    group = "spark"
    description = "Module 1.6.5 - Shuffle Hash Join experiment"

    classpath = sourceSets["main"].runtimeClasspath
    mainClass.set("com.shrikant.spark.joins.ShuffleHashJoinExperiment")

    jvmArgs(
        "--add-opens=java.base/java.lang=ALL-UNNAMED",
        "--add-exports=java.base/sun.nio.ch=ALL-UNNAMED"
    )
}

tasks.register<JavaExec>("runJoinHintsExperiment") {
    group = "spark"
    description = "Module 1.6.6 - Join Hints experiment"

    classpath = sourceSets["main"].runtimeClasspath
    mainClass.set("com.shrikant.spark.joins.JoinHintsExperiment")

    jvmArgs(
        "--add-opens=java.base/java.lang=ALL-UNNAMED",
        "--add-exports=java.base/sun.nio.ch=ALL-UNNAMED"
    )
}

tasks.register<JavaExec>("runBuildSideSelectionExperiment") {
    group = "spark"
    description = "Module 1.6.7 - Build Side Selection experiment"

    classpath = sourceSets["main"].runtimeClasspath
    mainClass.set("com.shrikant.spark.joins.BuildSideSelectionExperiment")

    jvmArgs(
        "--add-opens=java.base/java.lang=ALL-UNNAMED",
        "--add-exports=java.base/sun.nio.ch=ALL-UNNAMED"
    )
}

tasks.register<JavaExec>("runJoinStrategyComparisonExperiment") {
    group = "spark"
    description = "Module 1.6.8 - Join Strategy Comparison experiment"

    classpath = sourceSets["main"].runtimeClasspath
    mainClass.set("com.shrikant.spark.joins.JoinStrategyComparisonExperiment")

    jvmArgs(
        "--add-opens=java.base/java.lang=ALL-UNNAMED",
        "--add-exports=java.base/sun.nio.ch=ALL-UNNAMED"
    )
}

tasks.register<JavaExec>("runJoinSkewExperiment") {
    group = "spark"
    description = "Module 1.6.9 - Join Skew experiment"

    classpath = sourceSets["main"].runtimeClasspath
    mainClass.set("com.shrikant.spark.joins.JoinSkewExperiment")

    jvmArgs(
        "--add-opens=java.base/java.lang=ALL-UNNAMED",
        "--add-exports=java.base/sun.nio.ch=ALL-UNNAMED"
    )
}

tasks.register<JavaExec>("runAQESkewJoinExperiment") {
    group = "spark"
    description = "Module 1.6.10 - AQE + Join Skew experiment"

    classpath = sourceSets["main"].runtimeClasspath
    mainClass.set("com.shrikant.spark.joins.AQESkewJoinExperiment")

    jvmArgs(
        "--add-opens=java.base/java.lang=ALL-UNNAMED",
        "--add-exports=java.base/sun.nio.ch=ALL-UNNAMED"
    )
}

tasks.register<JavaExec>("runAQESkewEligibilityDiagnostic") {
    group = "spark"
    description = "Module 1.6.10A - AQE Skew Eligibility Diagnostic"

    classpath = sourceSets["main"].runtimeClasspath
    mainClass.set(
        "com.shrikant.spark.joins.AQESkewEligibilityDiagnostic"
    )

    jvmArgs(
        "--add-opens=java.base/java.lang=ALL-UNNAMED",
        "--add-exports=java.base/sun.nio.ch=ALL-UNNAMED"
    )
}

tasks.register<JavaExec>("runAQEJoinConversionExperiment") {
    group = "spark"
    description = "Module 1.6.11 - AQE Join Conversion experiment"

    classpath = sourceSets["main"].runtimeClasspath
    mainClass.set(
        "com.shrikant.spark.joins.AQEJoinConversionExperiment"
    )

    jvmArgs(
        "--add-opens=java.base/java.lang=ALL-UNNAMED",
        "--add-exports=java.base/sun.nio.ch=ALL-UNNAMED"
    )
}

tasks.register<JavaExec>("runProductionJoinOptimizationExperiment") {
    group = "spark"
    description = "Module 1.6.12 - Production Join Optimization experiment"

    classpath = sourceSets["main"].runtimeClasspath
    mainClass.set("com.shrikant.spark.joins.ProductionJoinOptimizationExperiment")

    jvmArgs(
        "--add-opens=java.base/java.lang=ALL-UNNAMED",
        "--add-exports=java.base/sun.nio.ch=ALL-UNNAMED"
    )
}

tasks.register<JavaExec>("runBaselineAggregationExperiment") {
    group = "spark"
    description = "Run Module 1.7.1 baseline aggregation experiment"

    classpath = sourceSets["main"].runtimeClasspath
    mainClass.set("com.shrikant.spark.aggregations.BaselineAggregationExperiment")

    jvmArgs(
        "--add-opens=java.base/java.lang=ALL-UNNAMED",
        "--add-exports=java.base/sun.nio.ch=ALL-UNNAMED"
    )
}

tasks.register<JavaExec>("runHashAggregationExperiment") {
    group = "spark"
    description = "Run Module 1.7.2 Hash Aggregation experiment"

    classpath = sourceSets["main"].runtimeClasspath
    mainClass.set("com.shrikant.spark.aggregations.HashAggregationExperiment")

    jvmArgs(
        "--add-opens=java.base/java.lang=ALL-UNNAMED",
        "--add-exports=java.base/sun.nio.ch=ALL-UNNAMED"
    )
}

tasks.register<JavaExec>("runPartialAggregationExperiment") {
    group = "spark"
    description = "Run Module 1.7.3 Partial Aggregation experiment"

    classpath = sourceSets["main"].runtimeClasspath
    mainClass.set("com.shrikant.spark.aggregations.PartialAggregationExperiment")

    jvmArgs(
        "--add-opens=java.base/java.lang=ALL-UNNAMED",
        "--add-exports=java.base/sun.nio.ch=ALL-UNNAMED"
    )
}

tasks.register<JavaExec>("runAggregationCardinalityExperiment") {
    group = "spark"
    description = "Run Module 1.7.4 Aggregation Cardinality experiment"

    classpath = sourceSets["main"].runtimeClasspath
    mainClass.set("com.shrikant.spark.aggregations.AggregationCardinalityExperiment")

    jvmArgs(
        "--add-opens=java.base/java.lang=ALL-UNNAMED",
        "--add-exports=java.base/sun.nio.ch=ALL-UNNAMED"
    )
}

tasks.register<JavaExec>("runShufflePartitionSizingExperiment") {
    group = "spark"
    description = "Run Module 1.7.5 Shuffle Partition Sizing experiment"

    classpath = sourceSets["main"].runtimeClasspath
    mainClass.set(
        "com.shrikant.spark.aggregations.ShufflePartitionSizingExperiment"
    )

    jvmArgs(
        "--add-opens=java.base/java.lang=ALL-UNNAMED",
        "--add-exports=java.base/sun.nio.ch=ALL-UNNAMED"
    )
}

tasks.register<JavaExec>("runAQECoalescingExperiment") {
    group = "spark"
    description = "Run Module 1.7.6 - AQE Coalescing Experiment"

    classpath = sourceSets["main"].runtimeClasspath
    mainClass.set("com.shrikant.spark.aggregations.AQECoalescingExperiment")

    standardInput = System.`in`

    jvmArgs(
        "--add-opens=java.base/java.lang=ALL-UNNAMED",
        "--add-exports=java.base/sun.nio.ch=ALL-UNNAMED"
    )
}

tasks.register<JavaExec>("runGroupByOrderByExperiment") {
    group = "spark"
    description = "Run Module 1.7.7 GroupBy + OrderBy experiment"

    classpath = sourceSets["main"].runtimeClasspath
    mainClass.set("com.shrikant.spark.aggregations.GroupByOrderByExperiment")

    jvmArgs(
        "--add-opens=java.base/java.lang=ALL-UNNAMED",
        "--add-exports=java.base/sun.nio.ch=ALL-UNNAMED"
    )

    standardInput = System.`in`
}

tasks.register<JavaExec>("runTopNAggregationExperiment") {
    group = "spark"
    description = "Run Module 1.7.8 Top-N Aggregation experiment"

    classpath = sourceSets["main"].runtimeClasspath
    mainClass.set("com.shrikant.spark.aggregations.TopNAggregationExperiment")

    jvmArgs(
        "--add-opens=java.base/java.lang=ALL-UNNAMED",
        "--add-exports=java.base/sun.nio.ch=ALL-UNNAMED"
    )

    standardInput = System.`in`
}

tasks.register<JavaExec>("runDataSkewAggregationExperiment") {
    group = "spark"
    description = "Run Module 1.7.9 Data Skew in Aggregation experiment"

    classpath = sourceSets["main"].runtimeClasspath
    mainClass.set("com.shrikant.spark.aggregations.DataSkewAggregationExperiment")

    jvmArgs(
        "--add-opens=java.base/java.lang=ALL-UNNAMED",
        "--add-exports=java.base/sun.nio.ch=ALL-UNNAMED"
    )

    standardInput = System.`in`
}

tasks.register<JavaExec>("runAggregationAQESkewExperiment") {
    group = "spark-experiments"
    description = "Run Module 1.7.10 - AQE + Aggregation Skew experiment"

    classpath = sourceSets["main"].runtimeClasspath
    mainClass.set("com.shrikant.spark.aggregations.AggregationAQESkewExperiment")

    args = project.findProperty("args")
        ?.toString()
        ?.split(" ")
        ?: emptyList()

    standardInput = System.`in`

    jvmArgs(
        "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
        "--add-opens=java.base/java.lang=ALL-UNNAMED",
        "--add-opens=java.base/java.nio=ALL-UNNAMED"
    )
}

tasks.register<JavaExec>("runAggregationStrategyComparison") {

    group = "spark"
    description = "Run Module 1.7.11 Aggregation Strategy Comparison"

    classpath = sourceSets["main"].runtimeClasspath
    mainClass.set("com.shrikant.spark.aggregations.AggregationStrategyComparison")

    // -------------------------------------------------------------------------
    // Forward the terminal's STDIN to the Java application.
    //
    // This is required because the Scala application pauses and waits for
    // ENTER before continuing to the next experiment.
    //
    // Without this line, System.in inside the Spark application may not be
    // connected to the Git Bash / terminal input when launched through Gradle.
    // -------------------------------------------------------------------------
    standardInput = System.`in`

    // -------------------------------------------------------------------------
    // Windows / Git Bash / MINGW compatibility.
    //
    // Spark on Java 17 can require access to JDK internals used by Spark /
    // Hadoop / Netty.
    //
    // The first argument also removes the MINGW warning:
    //
    //   MINGW support requires
    //   --add-opens java.base/java.lang=ALL-UNNAMED
    // -------------------------------------------------------------------------
    jvmArgs(
        "--add-opens=java.base/java.lang=ALL-UNNAMED",
        "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
        "--add-opens=java.base/java.nio=ALL-UNNAMED"
    )
}

tasks.register<JavaExec>("runAggregationMemorySpillExperiment") {
    group = "spark"
    description = "Run Module 1.7.12 Aggregation Memory, Spill & Hash Map Pressure experiment"

    classpath = sourceSets["main"].runtimeClasspath
    mainClass.set("com.shrikant.spark.aggregations.AggregationMemorySpillExperiment")

    jvmArgs(
        "--add-opens=java.base/java.lang=ALL-UNNAMED",
        "--add-exports=java.base/sun.nio.ch=ALL-UNNAMED"
    )

    standardInput = System.`in`
}

tasks.register<JavaExec>("runShuffleArchitectureExperiment") {

    group = "spark experiments"
    description = "Runs Module 1.8.1 - Shuffle Architecture & Data Movement"

    classpath = sourceSets["main"].runtimeClasspath

    mainClass.set(
        "com.shrikant.spark.shuffle.ShuffleArchitectureExperiment"
    )

    // Required so the launched Spark application has a normal stdin/stdout
    // process environment and is consistent with the repository's other
    // interactive/UI-enabled experiments.
    standardInput = System.`in`

    // Required by Spark when running locally with the JDK configuration used
    // by this repository.
    jvmArgs(
        "--add-opens=java.base/java.lang=ALL-UNNAMED",
        "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
        "--add-opens=java.base/java.nio=ALL-UNNAMED"
    )
}

tasks.register<JavaExec>("runShufflePartitionSizing") {
    group = "spark experiments"
    description = "Runs Module 1.8.2 - Shuffle Partition Sizing Experiment"

    classpath = sourceSets["main"].runtimeClasspath
    mainClass.set(
        "com.shrikant.spark.shuffle.ShufflePartitionSizingExperiment"
    )

    // Required so --ui-pause can receive ENTER from Git Bash / MINGW.
    standardInput = System.`in`

    // Required for Spark 3.5.1 execution under the current Java/MINGW setup.
    jvmArgs(
        "--add-opens=java.base/java.lang=ALL-UNNAMED",
        "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
        "--add-opens=java.base/java.nio=ALL-UNNAMED"
    )
}

tasks.register<JavaExec>("runShuffleReadWritePerformanceExperiment") {
    group = "spark"
    description = "Runs Module 1.8.3 - Shuffle Read/Write Performance"

    classpath = sourceSets["main"].runtimeClasspath

    mainClass.set(
        "com.shrikant.spark.shuffle.ShuffleReadWritePerformanceExperiment"
    )

    standardInput = System.`in`

    jvmArgs(
        "--add-opens=java.base/java.lang=ALL-UNNAMED",
        "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
        "--add-opens=java.base/java.nio=ALL-UNNAMED"
    )
}

tasks.register<JavaExec>("runShuffleSpillMemoryExperiment") {
    group = "application"
    description = "Module 1.8.4 - Shuffle Spill & Memory experiment"

    mainClass.set("com.shrikant.spark.shuffle.ShuffleSpillMemoryExperiment")
    classpath = sourceSets["main"].runtimeClasspath

    maxHeapSize = "512m"

    jvmArgs = listOf(
        "--add-opens=java.base/java.lang=ALL-UNNAMED",
        "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED",
        "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
        "--add-opens=java.base/java.nio=ALL-UNNAMED"
    )

    standardInput = System.`in`

    if (project.hasProperty("args")) {
        args = (project.property("args") as String).split(" ")
    }
}

tasks.register<JavaExec>("runShuffleSerializationCompressionExperiment") {
    group = "application"
    description = "Runs Module 1.8.5 — Serialization & Compression Experiment"
    mainClass.set("com.shrikant.spark.shuffle.ShuffleSerializationCompressionExperiment")
    classpath = sourceSets["main"].runtimeClasspath

    // JVM module opens required for Java 17 and Spark reflection/serialization
    jvmArgs(
        "--add-opens=java.base/java.lang=ALL-UNNAMED",
        "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED",
        "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED",
        "--add-opens=java.base/java.io=ALL-UNNAMED",
        "--add-opens=java.base/java.net=ALL-UNNAMED",
        "--add-opens=java.base/java.nio=ALL-UNNAMED",
        "--add-opens=java.base/java.util=ALL-UNNAMED",
        "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED",
        "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED",
        "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
        "--add-opens=java.base/sun.nio.cs=ALL-UNNAMED",
        "--add-opens=java.base/sun.security.action=ALL-UNNAMED",
        "--add-opens=java.util/java.util=ALL-UNNAMED"
    )

    // Pass CLI arguments passed to gradle via --args="..."
    val rawArgs = project.findProperty("args") as? String
    if (rawArgs != null) {
        setArgs(rawArgs.split(" "))
    }
}

tasks.register<JavaExec>("runShufflePushBasedExperiment") {
    group = "application"
    description = "Runs Module 1.8.6 — Push-Based Shuffle Experiment"
    mainClass.set("com.shrikant.spark.shuffle.ShufflePushBasedExperiment")
    classpath = sourceSets["main"].runtimeClasspath

    jvmArgs(
        "--add-opens=java.base/java.lang=ALL-UNNAMED",
        "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED",
        "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED",
        "--add-opens=java.base/java.io=ALL-UNNAMED",
        "--add-opens=java.base/java.net=ALL-UNNAMED",
        "--add-opens=java.base/java.nio=ALL-UNNAMED",
        "--add-opens=java.base/java.util=ALL-UNNAMED",
        "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED",
        "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED",
        "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
        "--add-opens=java.base/sun.nio.cs=ALL-UNNAMED",
        "--add-opens=java.base/sun.security.action=ALL-UNNAMED"
    )

    val rawArgs = project.findProperty("args") as? String
    if (rawArgs != null) {
        setArgs(rawArgs.split(" "))
    }
}

tasks.register<JavaExec>("runShuffleAQEExperiment") {
    group = "application"
    description = "Runs Module 1.8.7 — Adaptive Query Execution Experiment"
    mainClass.set("com.shrikant.spark.shuffle.ShuffleAQEExperiment")
    classpath = sourceSets["main"].runtimeClasspath

    jvmArgs(
        "--add-opens=java.base/java.lang=ALL-UNNAMED",
        "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED",
        "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED",
        "--add-opens=java.base/java.io=ALL-UNNAMED",
        "--add-opens=java.base/java.net=ALL-UNNAMED",
        "--add-opens=java.base/java.nio=ALL-UNNAMED",
        "--add-opens=java.base/java.util=ALL-UNNAMED",
        "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED",
        "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED",
        "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
        "--add-opens=java.base/sun.nio.cs=ALL-UNNAMED",
        "--add-opens=java.base/sun.security.action=ALL-UNNAMED"
    )

    val rawArgs = project.findProperty("args") as? String
    if (rawArgs != null) {
        setArgs(rawArgs.split(" "))
    }
}

tasks.register<JavaExec>("runMemoryTungsten") {
    group = "Spark Internals - Performance"
    description = "Runs Module 1.9.1 Memory Tungsten Off-Heap vs Heap Experiment"
    classpath = sourceSets["main"].runtimeClasspath
    mainClass.set("com.shrikant.spark.internals.performance.MemoryTungstenExperiment")

    val caseArg = project.findProperty("case")?.toString() ?: "A"
    args("--case=$caseArg")

    jvmArgs = listOf(
        // Required module exports for Spark 3.5.1 on Java 17
        "--add-exports=java.base/sun.nio.ch=ALL-UNNAMED",
        // Required module opens
        "--add-opens=java.base/java.lang=ALL-UNNAMED",
        "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED",
        "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED",
        "--add-opens=java.base/java.io=ALL-UNNAMED",
        "--add-opens=java.base/java.net=ALL-UNNAMED",
        "--add-opens=java.base/java.nio=ALL-UNNAMED",
        "--add-opens=java.base/java.util=ALL-UNNAMED",
        "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED",
        "--add-opens=java.base/java.util.concurrent.locks=ALL-UNNAMED",
        "--add-opens=java.base/java.text=ALL-UNNAMED",
        "--add-opens=java.sql/java.sql=ALL-UNNAMED"
    )
}

tasks.register<JavaExec>("runTungstenBinaryRow") {
    group = "Spark Internals - Performance"
    description = "Runs Module 1.9.2 Tungsten Binary Row Format Experiment"
    classpath = sourceSets["main"].runtimeClasspath
    mainClass.set("com.shrikant.spark.internals.performance.TungstenBinaryRowExperiment")

    jvmArgs = listOf(
        "--add-exports=java.base/sun.nio.ch=ALL-UNNAMED",
        "--add-opens=java.base/java.lang=ALL-UNNAMED",
        "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED",
        "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED",
        "--add-opens=java.base/java.io=ALL-UNNAMED",
        "--add-opens=java.base/java.net=ALL-UNNAMED",
        "--add-opens=java.base/java.nio=ALL-UNNAMED",
        "--add-opens=java.base/java.util=ALL-UNNAMED",
        "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED",
        "--add-opens=java.base/java.util.concurrent.locks=ALL-UNNAMED",
        "--add-opens=java.base/java.text=ALL-UNNAMED",
        "--add-opens=java.sql/java.sql=ALL-UNNAMED"
    )
}

tasks.register<JavaExec>("runCatalystCustomRule") {
    group = "Spark Internals"
    description = "Runs Module 2.1 Catalyst Custom Rule Experiment"
    classpath = sourceSets["main"].runtimeClasspath
    mainClass.set("com.shrikant.spark.internals.catalyst.CatalystCustomRuleExperiment")

    // Pass standard Spark local execution environment flags if needed
    jvmArgs(
        "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
        "--add-opens=java.base/java.nio=ALL-UNNAMED",
        "--add-opens=java.base/java.lang=ALL-UNNAMED",
        "--add-opens=java.base/java.util=ALL-UNNAMED"
    )

    // Ensure standard output is connected so println and explain(true) show up in console
    standardInput = System.`in`
}

tasks.register<JavaExec>("runTungstenMemoryExperiment") {
    group = "Spark Internals"
    description = "Runs Module 2.2 Tungsten Off-Heap Memory Experiment"
    classpath = sourceSets["main"].runtimeClasspath
    mainClass.set("com.shrikant.spark.internals.tungsten.MemoryTungstenExperiment")

    jvmArgs(
        "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
        "--add-exports=java.base/sun.nio.ch=ALL-UNNAMED",
        "--add-opens=java.base/java.nio=ALL-UNNAMED",
        "--add-opens=java.base/sun.misc=ALL-UNNAMED"
    )
    standardInput = System.`in`
}

tasks.register<JavaExec>("runWholeStageCodegenExperiment") {
    group = "Spark Internals"
    description = "Runs Module 2.3 WholeStageCodegen & Janino Experiment"
    classpath = sourceSets["main"].runtimeClasspath
    mainClass.set("com.shrikant.spark.internals.codegen.WholeStageCodegenExperiment")

    jvmArgs(
        "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
        "--add-exports=java.base/sun.nio.ch=ALL-UNNAMED",
        "--add-opens=java.base/java.nio=ALL-UNNAMED",
        "--add-opens=java.base/sun.misc=ALL-UNNAMED"
    )

    standardInput = System.`in`
}

tasks.register<JavaExec>("runAQEExperiment") {
    group = "Spark Internals"
    description = "Runs Module 2.4 Adaptive Query Execution Experiment"
    classpath = sourceSets["main"].runtimeClasspath
    mainClass.set("com.shrikant.spark.internals.aqe.AQEExperiment")

    jvmArgs(
        "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
        "--add-exports=java.base/sun.nio.ch=ALL-UNNAMED",
        "--add-opens=java.base/java.nio=ALL-UNNAMED",
        "--add-opens=java.base/sun.misc=ALL-UNNAMED"
    )

    standardInput = System.`in`
}

tasks.register<JavaExec>("runShufflePartitionExperiment") {
    group = "Spark Performance Tuning"
    description = "Runs Module 3.1: Shuffle Architecture & Partition Tuning Experiment"
    classpath = sourceSets["main"].runtimeClasspath
    mainClass.set("com.shrikant.spark.performance.ShufflePartitionExperiment")
    jvmArgs(
        "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
        "--add-opens=java.base/java.util=ALL-UNNAMED",
        "--add-opens=java.base/java.lang=ALL-UNNAMED"
    )
}

tasks.register<JavaExec>("runJoinStrategyExperiment") {
    group = "Spark Performance Tuning"
    description = "Runs Module 3.2: Join Strategies & Cost-Based Optimization Experiment"
    classpath = sourceSets["main"].runtimeClasspath
    mainClass.set("com.shrikant.spark.performance.JoinStrategyExperiment")
    jvmArgs(
        "--add-opens=java.base/java.nio=ALL-UNNAMED",
        "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
        "--add-opens=java.base/java.lang=ALL-UNNAMED"
    )
}

tasks.register<JavaExec>("runDataSkewExperiment") {
    group = "Spark Performance Tuning"
    description = "Runs Module 3.3: Data Skew Identification, Salting, & AQE Skew Joins Experiment"
    classpath = sourceSets["main"].runtimeClasspath
    mainClass.set("com.shrikant.spark.performance.DataSkewExperiment")
    jvmArgs(
        "--add-opens=java.base/java.nio=ALL-UNNAMED",
        "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
        "--add-opens=java.base/java.lang=ALL-UNNAMED"
    )
}

tasks.register<JavaExec>("runMemoryManagementExperiment") {
    group = "Spark Performance Tuning"
    description = "Runs Module 3.4: Memory Management, Execution/Storage Pools Experiment"
    classpath = sourceSets["main"].runtimeClasspath
    mainClass.set("com.shrikant.spark.performance.MemoryManagementExperiment")
    jvmArgs(
        "--add-opens=java.base/java.nio=ALL-UNNAMED",
        "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
        "--add-opens=java.base/java.lang=ALL-UNNAMED"
    )
}

tasks.register<JavaExec>("runStreamingArchExperiment") {
    group = "spark-experiments"
    description = "Executes Module 4.1: Streaming Architecture, Triggers, & Observability"
    mainClass.set("streaming.StreamingArchitectureExperiment")
    classpath = sourceSets["main"].runtimeClasspath

    // Set Hadoop home directory for Windows execution
    val hadoopHome = "${System.getProperty("java.io.tmpdir")}\\hadoop-winutils"
    systemProperty("hadoop.home.dir", hadoopHome)

    // Corrected package name: sun.nio.ch (instead of java.nio.ch)
    jvmArgs = listOf(
        "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
        "--add-opens=java.base/java.lang=ALL-UNNAMED",
        "--add-opens=java.base/java.util=ALL-UNNAMED",
        "--add-opens=java.base/java.nio=ALL-UNNAMED"
    )
}

tasks.register<JavaExec>("runStateManagementExperiment") {
    group = "spark-experiments"
    description = "Executes Module 4.2: State Management, RocksDB & Watermarking"
    mainClass.set("streaming.StateManagementWatermarkingExperiment")
    classpath = sourceSets["main"].runtimeClasspath

    // Reuse the Windows Hadoop home directory setup
    val hadoopHome = "${System.getProperty("java.io.tmpdir")}\\hadoop-winutils"
    systemProperty("hadoop.home.dir", hadoopHome)

    jvmArgs = listOf(
        "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
        "--add-opens=java.base/java.lang=ALL-UNNAMED",
        "--add-opens=java.base/java.util=ALL-UNNAMED",
        "--add-opens=java.base/java.nio=ALL-UNNAMED"
    )
}

tasks.register<JavaExec>("runFaultToleranceExperiment") {
    group = "spark-experiments"
    description = "Executes Module 4.3: Fault Tolerance & DLQ Patterns"
    mainClass.set("streaming.FaultToleranceDLQExperiment")
    classpath = sourceSets["main"].runtimeClasspath

    val hadoopHome = "${System.getProperty("java.io.tmpdir")}\\hadoop-winutils"
    systemProperty("hadoop.home.dir", hadoopHome)

    jvmArgs = listOf(
        "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
        "--add-opens=java.base/java.lang=ALL-UNNAMED",
        "--add-opens=java.base/java.util=ALL-UNNAMED",
        "--add-opens=java.base/java.nio=ALL-UNNAMED"
    )
}

tasks.register<JavaExec>("runObservabilityExperiment") {
    group = "spark-experiments"
    description = "Executes Module 4.4: Production Observability & Metrics"
    mainClass.set("streaming.ObservabilityMetricsExperiment")
    classpath = sourceSets["main"].runtimeClasspath

    val hadoopHome = "${System.getProperty("java.io.tmpdir")}\\hadoop-winutils"
    systemProperty("hadoop.home.dir", hadoopHome)

    jvmArgs = listOf(
        "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
        "--add-opens=java.base/java.lang=ALL-UNNAMED",
        "--add-opens=java.base/java.util=ALL-UNNAMED",
        "--add-opens=java.base/java.nio=ALL-UNNAMED"
    )
}

tasks.register<JavaExec>("runStreamJoinExperiment") {
    group = "spark-experiments"
    description = "Executes Module 4.5: Stream-to-Stream Joins & State"
    mainClass.set("streaming.StreamJoinStatefulExperiment")
    classpath = sourceSets["main"].runtimeClasspath

    val hadoopHome = "${System.getProperty("java.io.tmpdir")}\\hadoop-winutils"
    systemProperty("hadoop.home.dir", hadoopHome)

    jvmArgs = listOf(
        "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
        "--add-opens=java.base/java.lang=ALL-UNNAMED",
        "--add-opens=java.base/java.util=ALL-UNNAMED",
        "--add-opens=java.base/java.nio=ALL-UNNAMED"
    )
}

tasks.register<JavaExec>("runLakeHouseExperiment") {
    group = "lakehouse-phase5"
    description = "Runs Module 5.1 LakeHouse Architecture Delta Lake Experiment"

    classpath = sourceSets["main"].runtimeClasspath
    mainClass.set("lakehouse.LakehouseExperiment")

    // Set working directory so spark-warehouse is created cleanly locally
    workingDir = projectDir

    // JVM arguments for Spark 3.x compatibility on Java 17+
    jvmArgs(
        "--add-opens=java.base/java.lang=ALL-UNNAMED",
        "--add-opens=java.base/java.invoke=ALL-UNNAMED",
        "--add-opens=java.base/java.io=ALL-UNNAMED",
        "--add-opens=java.base/java.util=ALL-UNNAMED",
        "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED",
        "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
        "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED",
        "-Xmx4G" // Allocate sufficient heap memory for Spark driver/executors
    )
}

tasks.register<JavaExec>("runCdcExperiment") {
    group = "lakehouse-phase5"
    description = "Runs Module 5.2 Incremental Processing & CDC Experiment"

    classpath = sourceSets["main"].runtimeClasspath
    mainClass.set("lakehouse.CdcExperiment")
    workingDir = projectDir

    jvmArgs(
        "--add-opens=java.base/java.lang=ALL-UNNAMED",
        "--add-opens=java.base/java.invoke=ALL-UNNAMED",
        "--add-opens=java.base/java.io=ALL-UNNAMED",
        "--add-opens=java.base/java.util=ALL-UNNAMED",
        "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED",
        "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
        "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED",
        "-Xmx4G"
    )
}

tasks.register<JavaExec>("runDataQualityExperiment") {
    group = "lakehouse-phase5"
    description = "Runs Module 5.3 Data Quality & Quarantine DLQ Experiment"

    classpath = sourceSets["main"].runtimeClasspath
    mainClass.set("lakehouse.DataQualityExperiment")
    workingDir = projectDir

    jvmArgs(
        "--add-opens=java.base/java.lang=ALL-UNNAMED",
        "--add-opens=java.base/java.invoke=ALL-UNNAMED",
        "--add-opens=java.base/java.io=ALL-UNNAMED",
        "--add-opens=java.base/java.util=ALL-UNNAMED",
        "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED",
        "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
        "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED",
        "-Xmx4G"
    )
}

tasks.register<JavaExec>("runSchemaEvolutionExperiment") {
    group = "lakehouse-phase5"
    description = "Runs Module 5.4 Schema Evolution & Data Compatibility Experiment"

    classpath = sourceSets["main"].runtimeClasspath
    mainClass.set("lakehouse.SchemaEvolutionExperiment")
    workingDir = projectDir

    jvmArgs(
        "--add-opens=java.base/java.lang=ALL-UNNAMED",
        "--add-opens=java.base/java.invoke=ALL-UNNAMED",
        "--add-opens=java.base/java.io=ALL-UNNAMED",
        "--add-opens=java.base/java.util=ALL-UNNAMED",
        "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED",
        "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
        "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED",
        "-Xmx4G"
    )
}

tasks.register<JavaExec>("runMedallionExperiment") {
    group = "lakehouse-phase5"
    description = "Runs Module 5.5 Medallion Architecture Multi-Hop Pipeline Experiment"

    classpath = sourceSets["main"].runtimeClasspath
    mainClass.set("lakehouse.MedallionPipelineExperiment")
    workingDir = projectDir

    jvmArgs(
        "--add-opens=java.base/java.lang=ALL-UNNAMED",
        "--add-opens=java.base/java.invoke=ALL-UNNAMED",
        "--add-opens=java.base/java.io=ALL-UNNAMED",
        "--add-opens=java.base/java.util=ALL-UNNAMED",
        "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED",
        "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
        "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED",
        "-Xmx4G"
    )
}

tasks.register<JavaExec>("runStreamingLakeHouseExperiment") {
    group = "lakehouse-phase5"
    description = "Runs Module 5.6 Structured Streaming with Delta Lake Experiment"

    classpath = sourceSets["main"].runtimeClasspath
    mainClass.set("lakehouse.StreamingLakehouseExperiment")
    workingDir = projectDir

    jvmArgs(
        "--add-opens=java.base/java.lang=ALL-UNNAMED",
        "--add-opens=java.base/java.invoke=ALL-UNNAMED",
        "--add-opens=java.base/java.io=ALL-UNNAMED",
        "--add-opens=java.base/java.util=ALL-UNNAMED",
        "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED",
        "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
        "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED",
        "-Xmx4G"
    )
}