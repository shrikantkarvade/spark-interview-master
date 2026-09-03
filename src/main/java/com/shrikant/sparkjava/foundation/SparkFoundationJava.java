package com.shrikant.sparkjava.foundation;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.Arrays;
import java.util.List;

import static org.apache.spark.sql.functions.*;

public class SparkFoundationJava {

    public static void main(String[] args) {

        SparkSession spark = SparkSession.builder()
                .appName("Module 1.2 - Spark Foundation - Java")
                .master("local[*]")
                .getOrCreate();

        spark.sparkContext().setLogLevel("WARN");

        System.out.println("========================================");
        System.out.println("   Module 1.2 - Spark Foundation Java");
        System.out.println("========================================");

        // ------------------------------------------------------------
        // 1. Create sample transaction data
        // ------------------------------------------------------------

        List<Row> rows = Arrays.asList(
                RowFactory.create("T001", "C001", "Pune", 15000.0),
                RowFactory.create("T002", "C002", "Mumbai", 25000.0),
                RowFactory.create("T003", "C001", "Pune", 50000.0),
                RowFactory.create("T004", "C003", "Delhi", 12000.0),
                RowFactory.create("T005", "C002", "Mumbai", 75000.0),
                RowFactory.create("T006", "C001", "Pune", 10000.0),
                RowFactory.create("T007", "C003", "Delhi", 90000.0),
                RowFactory.create("T008", "C002", "Mumbai", 15000.0)
        );

        StructType schema = new StructType(new StructField[]{
                DataTypes.createStructField(
                        "transaction_id",
                        DataTypes.StringType,
                        false
                ),
                DataTypes.createStructField(
                        "customer_id",
                        DataTypes.StringType,
                        false
                ),
                DataTypes.createStructField(
                        "city",
                        DataTypes.StringType,
                        false
                ),
                DataTypes.createStructField(
                        "amount",
                        DataTypes.DoubleType,
                        false
                )
        });

        Dataset<Row> transactionsDf =
                spark.createDataFrame(rows, schema);

        System.out.println("\n--- Source Data ---");

        transactionsDf.show();

        // ------------------------------------------------------------
        // 2. Transformation
        // ------------------------------------------------------------

        System.out.println("\n--- Defining Transformation ---");

        Dataset<Row> highValueTransactions =
                transactionsDf.filter(col("amount").geq(50000));

        System.out.println("Transformation defined.");
        System.out.println("No Spark computation has been triggered yet.");

        // ------------------------------------------------------------
        // 3. Action
        // ------------------------------------------------------------

        System.out.println("\n--- Executing Action ---");

        highValueTransactions.show();

        // ------------------------------------------------------------
        // 4. Aggregation
        // ------------------------------------------------------------

        System.out.println("\n--- Total Transaction Amount By Customer ---");

        Dataset<Row> customerTotals =
                transactionsDf
                        .groupBy("customer_id")
                        .agg(
                                sum("amount").alias("total_amount"),
                                count("*").alias("transaction_count")
                        )
                        .orderBy(desc("total_amount"));

        customerTotals.show();

        // ------------------------------------------------------------
        // 5. Explain execution plan
        // ------------------------------------------------------------

        System.out.println("\n--- Extended Execution Plan ---");

        customerTotals.explain(true);

        spark.stop();
    }
}