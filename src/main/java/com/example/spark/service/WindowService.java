package com.example.spark.service;

import org.apache.spark.sql.*;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.apache.spark.sql.types.StructField;
import org.springframework.stereotype.Service;

import java.util.*;

import static org.apache.spark.sql.functions.*;

/**
 * Window Functions — aggregate/analytic calculations over a sliding "window" of rows.
 *
 * A window is defined by:
 *   partitionBy : restart the calculation for each group (like GROUP BY, but rows aren't collapsed)
 *   orderBy     : the sort order within each partition
 *   frame spec  : which rows within the partition to include (rowsBetween / rangeBetween)
 *
 * Key difference from groupBy:
 *   groupBy collapses N rows → 1 row per group.
 *   Window functions keep all rows and add a new column computed across the window.
 *
 * Functions covered:
 *   rank()          : 1-based rank; ties get the same rank, next rank skips (1,1,3)
 *   dense_rank()    : like rank but no skipping (1,1,2)
 *   row_number()    : unique sequential number regardless of ties (1,2,3)
 *   sum() over()    : running total within partition
 *   lag(col, n)     : value n rows before current row (null at start)
 *   lead(col, n)    : value n rows after current row (null at end)
 */
@Service
public class WindowService {

    private final SparkSession spark;

    public WindowService(SparkSession spark) {
        this.spark = spark;
    }

    private String dataPath(String filename) {
        return getClass().getClassLoader().getResource("data/" + filename).getPath();
    }

    private Dataset<Row> loadEmployees() {
        return spark.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .csv(dataPath("employees.csv"));
    }

    // Example 1: Rank employees by salary within each department
    public List<Map<String, Object>> rankByDept() {
        Dataset<Row> df = loadEmployees();

        // Window: per-department, ordered by salary descending
        WindowSpec w = Window.partitionBy("department").orderBy(desc("salary"));

        Dataset<Row> result = df
                .withColumn("rank",        rank().over(w))         // gaps after ties
                .withColumn("dense_rank",  dense_rank().over(w))   // no gaps
                .withColumn("row_number",  row_number().over(w))   // always unique
                .select("department", "name", "salary", "rank", "dense_rank", "row_number")
                .orderBy("department", "rank");

        return rowsToMaps(result);
    }

    // Example 2: Running total — cumulative salary sum per department
    public List<Map<String, Object>> runningTotal() {
        Dataset<Row> df = loadEmployees();

        // Frame: from the very first row in the partition up to (and including) the current row
        WindowSpec w = Window.partitionBy("department")
                .orderBy("salary")
                .rowsBetween(Window.unboundedPreceding(), Window.currentRow());

        Dataset<Row> result = df
                .withColumn("running_total", sum("salary").over(w))
                .select("department", "name", "salary", "running_total")
                .orderBy("department", "salary");

        return rowsToMaps(result);
    }

    // Example 3: Lag / Lead — compare each employee's salary to neighbours in dept
    public List<Map<String, Object>> lagLead() {
        Dataset<Row> df = loadEmployees();

        WindowSpec w = Window.partitionBy("department").orderBy("salary");

        // Define lag/lead columns separately so we can reuse them in diff calculation
        Column prevSalary = lag("salary", 1).over(w);
        Column nextSalary = lead("salary", 1).over(w);

        Dataset<Row> result = df
                .withColumn("prev_salary",     prevSalary)           // null for first in dept
                .withColumn("next_salary",     nextSalary)           // null for last in dept
                .withColumn("diff_from_prev",  col("salary").minus(prevSalary))  // null for first
                .select("department", "name", "salary",
                        "prev_salary", "next_salary", "diff_from_prev")
                .orderBy("department", "salary");

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
