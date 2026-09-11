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