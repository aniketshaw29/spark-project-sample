package com.example.spark.service;

import org.apache.spark.sql.*;
import org.apache.spark.sql.types.StructField;
import org.springframework.stereotype.Service;

import java.util.*;

import static org.apache.spark.sql.functions.*;

/**
 * DataFrame API — a distributed table with named columns and a schema.
 *
 * Key concepts demonstrated:
 *   - spark.read().csv()       : load CSV into a DataFrame
 *   - inferSchema              : Spark auto-detects column types
 *   - createOrReplaceTempView : register DataFrame so it can be queried with SQL
 *   - spark.sql(...)           : run standard SQL on registered views
 *   - groupBy().agg()          : aggregate functions (count, avg, max, min, sum)
 *   - spark.read().json()      : load newline-delimited JSON
 *
 * Why DataFrames over RDDs?
 *   Spark's query planner (Catalyst) can optimise DataFrames the same way a database
 *   optimises SQL — pushing filters down, reordering joins, etc. RDDs get no such help.
 */
@Service
public class DataFrameService {

    private final SparkSession spark;

    public DataFrameService(SparkSession spark) {
        this.spark = spark;
    }

    private String dataPath(String filename) {
        return getClass().getClassLoader().getResource("data/" + filename).getPath();
    }

    // Example 1: Load CSV, inspect schema, peek at rows
    public Map<String, Object> loadAndInspect() {
        Dataset<Row> df = spark.read()
                .option("header", "true")       // treat first row as column names
                .option("inferSchema", "true")  // auto-detect int / double / string
                .csv(dataPath("employees.csv"));

        List<String> schema = new ArrayList<>();
        for (StructField field : df.schema().fields()) {
            schema.add(field.name() + " : " + field.dataType().simpleString());
        }

        Map<String, Object> result = new LinkedHashMap<>();
        result.put("total_rows", df.count());
        result.put("schema", schema);
        result.put("sample_rows", rowsToMaps(df.limit(5)));
        return result;
    }

    // Example 2: SQL on a DataFrame (register as a temp view first)
    public List<Map<String, Object>> sqlQuery() {
        Dataset<Row> df = spark.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .csv(dataPath("employees.csv"));

        // createOrReplaceTempView: makes this DataFrame available as "employees" in SQL
        // It's session-scoped — the view disappears when the SparkSession ends
        df.createOrReplaceTempView("employees");

        Dataset<Row> result = spark.sql(
                "SELECT name, department, salary " +
                "FROM employees " +
                "WHERE salary > 80000 " +
                "ORDER BY salary DESC"
        );

        return rowsToMaps(result);
    }

    // Example 3: GroupBy + multiple aggregations
    public List<Map<String, Object>> deptSummary() {
        Dataset<Row> df = spark.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .csv(dataPath("employees.csv"));

        // groupBy().agg() is the DataFrame equivalent of GROUP BY in SQL
        // The static imports from functions.* provide count(), avg(), max(), etc.
        Dataset<Row> summary = df
                .groupBy("department")
                .agg(
                        count("id").alias("employee_count"),
                        avg("salary").alias("avg_salary"),
                        max("salary").alias("max_salary"),
                        min("age").alias("youngest_age")
                )
                .orderBy(desc("avg_salary"));

        return rowsToMaps(summary);
    }

    // Example 4: Read newline-delimited JSON + groupBy two columns
    public List<Map<String, Object>> querySales() {
        // Spark reads JSON as newline-delimited JSON (one object per line), not JSON arrays
        Dataset<Row> sales = spark.read()
                .json(dataPath("sales.json"));

        Dataset<Row> result = sales
                .groupBy("region", "quarter")
                .agg(
                        count("id").alias("transactions"),
                        sum("amount").alias("total_revenue"),
                        avg("amount").alias("avg_transaction")
                )
                .orderBy("quarter", "region");

        return rowsToMaps(result);
    }

    private List<Map<String, Object>> rowsToMaps(Dataset<Row> dataset) {
        List<Map<String, Object>> result = new ArrayList<>();
        for (Row row : dataset.collectAsList()) {
            Map<String, Object> map = new LinkedHashMap<>();
            for (StructField field : dataset.schema().fields()) {
                Object val = row.getAs(field.name());
                if (val instanceof Double d) val = Math.round(d * 100.0) / 100.0;
                map.put(field.name(), val);
            }
            result.add(map);
        }
        return result;
    }
}
