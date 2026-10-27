package com.example.spark.service;

import com.example.spark.model.Employee;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.ReduceFunction;
import org.apache.spark.sql.*;
import org.springframework.stereotype.Service;

import java.util.List;

import static org.apache.spark.sql.functions.*;

/**
 * Dataset<T> API — a typed DataFrame backed by Java beans.
 *
 * Key concepts demonstrated:
 *   - Encoders.bean(T.class)   : tells Spark how to serialize/deserialize your POJO
 *   - df.as(encoder)           : convert an untyped DataFrame → typed Dataset<T>
 *   - FilterFunction<T>        : type-safe filter (compile-time checked)
 *   - MapFunction<T, U>        : type-safe map (Employee → String)
 *   - ReduceFunction<T>        : type-safe reduce (fold all rows into one)
 *
 * DataFrame vs Dataset:
 *   DataFrame  = Dataset<Row>  — untyped, any Column error is a runtime error
 *   Dataset<T> = typed         — column mismatch is a compile-time error
 *
 * Tradeoff: Datasets are safer but slightly less performant than DataFrames because
 * Spark must deserialize rows into Java objects for lambda execution.
 */
@Service
public class DatasetService {

    private final SparkSession spark;

    public DatasetService(SparkSession spark) {
        this.spark = spark;
    }

    private String dataPath(String filename) {
        return getClass().getClassLoader().getResource("data/" + filename).getPath();
    }

    private Dataset<Employee> loadEmployees() {
        return spark.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .csv(dataPath("employees.csv"))
                // cast salary to double to match the Employee bean field type;
                // inferSchema reads whole-number values as int/long by default
                .withColumn("salary", col("salary").cast("double"))
                // as(encoder): convert Dataset<Row> → Dataset<Employee>
                // Spark maps each column name to the matching getter/setter in the bean
                .as(Encoders.bean(Employee.class));
    }

    // Example 1: Typed filter — compile-time safety for predicate
    public List<Employee> filterHighEarners(double minSalary) {
        return loadEmployees()
                .filter((FilterFunction<Employee>) e -> e.getSalary() >= minSalary)
                .orderBy(desc("salary"))
                .collectAsList();
    }

    // Example 2: Typed map — transform each Employee to a formatted String
    public List<String> employeeSummaries() {
        return loadEmployees()
                .map(
                        (MapFunction<Employee, String>) e ->
                                String.format("%s (%s) — $%.0f, age %d",
                                        e.getName(), e.getDepartment(), e.getSalary(), e.getAge()),
                        Encoders.STRING()
                )
                .collectAsList();
    }

    // Example 3: Typed reduce — find the highest-paid employee
    public Employee highestPaid() {
        // reduce folds the entire dataset into a single value using a binary function
        return loadEmployees()
                .reduce((ReduceFunction<Employee>) (a, b) ->
                        a.getSalary() >= b.getSalary() ? a : b
                );
    }
}
