package com.example.spark.service;

import org.apache.spark.sql.*;
import org.apache.spark.sql.types.StructField;
import org.springframework.stereotype.Service;

import java.util.*;

/**
 * DataFrame Joins — combining two datasets on a common key.
 *
 * Join types covered:
 *   inner      : only rows with a match in BOTH tables
 *   left       : all rows from left table; nulls for non-matching right columns
 *   left_anti  : rows from left that have NO match in right (inverse of inner)
 *
 * Other join types Spark supports (not shown here):
 *   right, full (outer), cross, left_semi
 *
 * Data:
 *   employees.csv  — 10 employees across Engineering / Marketing / HR
 *   departments.csv — 4 departments: Engineering, Marketing, HR, Finance
 *                     Finance has no employees → useful for anti-join demo
 */
@Service
public class JoinService {

    private final SparkSession spark;

    public JoinService(SparkSession spark) {
        this.spark = spark;
    }

    private String dataPath(String filename) {
        return getClass().getClassLoader().getResource("data/" + filename).getPath();
    }

    private Dataset<Row> employees() {
        return spark.read().option("header", "true").option("inferSchema", "true")
                .csv(dataPath("employees.csv"));
    }

    private Dataset<Row> departments() {
        return spark.read().option("header", "true").option("inferSchema", "true")
                .csv(dataPath("departments.csv"));
    }

    // Example 1: Inner join — employees enriched with dept metadata (only matched rows)
    public List<Map<String, Object>> innerJoin() {
        Dataset<Row> emp  = employees();
        Dataset<Row> dept = departments();

        // join(rightDataset, condition, joinType)
        // Use .col() to disambiguate when both tables share a column name
        Dataset<Row> result = emp
                .join(dept, emp.col("department").equalTo(dept.col("name")), "inner")
                .select(
                        emp.col("name").alias("employee"),
                        emp.col("department"),
                        emp.col("salary"),
                        dept.col("budget"),
                        dept.col("location")
                )
                .orderBy("department", "salary");

        return rowsToMaps(result);
    }

    // Example 2: Left join — all employees, null dept columns if dept not in departments table
    public List<Map<String, Object>> leftJoin() {
        Dataset<Row> emp  = employees();
        Dataset<Row> dept = departments();

        // Add a fake employee in a non-existent dept to demo the nulls
        Dataset<Row> extraEmp = spark.createDataFrame(
                List.of(new org.apache.spark.sql.Row[] {
                        org.apache.spark.sql.RowFactory.create(11, "Zara", "Legal", 90000.0, 31)
                }),
                emp.schema()
        );
        Dataset<Row> allEmps = emp.union(extraEmp);

        Dataset<Row> result = allEmps
                .join(dept, allEmps.col("department").equalTo(dept.col("name")), "left")
                .select(
                        allEmps.col("name"),
                        allEmps.col("department"),
                        dept.col("budget"),    // null for "Legal" — dept not in departments.csv
                        dept.col("location")   // null for "Legal"
                )
                .orderBy("department");

        return rowsToMaps(result);
    }

    // Example 3: Anti join — departments that have NO employees
    public List<Map<String, Object>> antiJoin() {
        Dataset<Row> emp  = employees();
        Dataset<Row> dept = departments();

        // left_anti: keep rows from dept where there is NO matching row in emp
        // Finance exists in departments.csv but has no employees → it appears here
        Dataset<Row> result = dept
                .join(emp, dept.col("name").equalTo(emp.col("department")), "left_anti")
                .select("name", "budget", "location");

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
