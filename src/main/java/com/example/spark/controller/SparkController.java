package com.example.spark.controller;

import com.example.spark.model.Employee;
import com.example.spark.service.DataFrameService;
import com.example.spark.service.DatasetService;
import com.example.spark.service.RddService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.*;

/**
 * REST endpoints — one per Spark concept.
 *
 * RDD endpoints:
 *   GET /api/spark/rdd/word-count?text=...   word frequency (map + countByValue)
 *   GET /api/spark/rdd/numbers               map / filter / reduce on 1–100
 *   GET /api/spark/rdd/flatmap               flatMap: sentences → words
 *
 * DataFrame endpoints:
 *   GET /api/spark/dataframe/schema          load CSV, print schema + sample rows
 *   GET /api/spark/dataframe/sql             SQL on a registered temp view
 *   GET /api/spark/dataframe/dept-summary    groupBy + agg (count, avg, max)
 *   GET /api/spark/dataframe/sales           JSON file + groupBy two columns
 *
 * Dataset endpoints (typed):
 *   GET /api/spark/dataset/high-earners?minSalary=80000   FilterFunction<Employee>
 *   GET /api/spark/dataset/summaries                       MapFunction<Employee, String>
 *   GET /api/spark/dataset/highest-paid                    ReduceFunction<Employee>
 */
@RestController
@RequestMapping("/api/spark")
public class SparkController {

    private final RddService rddService;
    private final DataFrameService dataFrameService;
    private final DatasetService datasetService;

    public SparkController(RddService rddService,
                           DataFrameService dataFrameService,
                           DatasetService datasetService) {
        this.rddService = rddService;
        this.dataFrameService = dataFrameService;
        this.datasetService = datasetService;
    }

    // ── RDD endpoints ─────────────────────────────────────────────────────────

    @GetMapping("/rdd/word-count")
    public ResponseEntity<Map<String, Object>> wordCount(
            @RequestParam(defaultValue =
                    "to be or not to be that is the question whether tis nobler in the mind to suffer " +
                    "the slings and arrows of outrageous fortune or to take arms against a sea of troubles")
            String text) {

        Map<String, Object> response = new LinkedHashMap<>();
        response.put("concept", "RDD: parallelize text → map(lowercase) → filter(letters) → countByValue()");
        response.put("top_words", rddService.wordCount(text));
        return ResponseEntity.ok(response);
    }

    @GetMapping("/rdd/numbers")
    public ResponseEntity<Map<String, Object>> numberOps() {
        Map<String, Object> result = rddService.numberOps();
        result.put("concept", "RDD: parallelize list → filter(even) → map(square) → reduce(sum)");
        return ResponseEntity.ok(result);
    }

    @GetMapping("/rdd/flatmap")
    public ResponseEntity<Map<String, Object>> flatMap() {
        Map<String, Object> result = rddService.flatMapExample();
        result.put("concept", "RDD flatMap: each sentence (1 element) → multiple words (N elements)");
        return ResponseEntity.ok(result);
    }

    // ── DataFrame endpoints ───────────────────────────────────────────────────

    @GetMapping("/dataframe/schema")
    public ResponseEntity<Map<String, Object>> schema() {
        Map<String, Object> result = dataFrameService.loadAndInspect();
        result.put("concept", "DataFrame: load CSV with inferSchema, inspect column types");
        return ResponseEntity.ok(result);
    }

    @GetMapping("/dataframe/sql")
    public ResponseEntity<Map<String, Object>> sql() {
        Map<String, Object> response = new LinkedHashMap<>();
        response.put("concept", "DataFrame: createOrReplaceTempView → run standard SQL");
        response.put("query", "SELECT name, department, salary FROM employees WHERE salary > 80000 ORDER BY salary DESC");
        response.put("results", dataFrameService.sqlQuery());
        return ResponseEntity.ok(response);
    }

    @GetMapping("/dataframe/dept-summary")
    public ResponseEntity<Map<String, Object>> deptSummary() {
        Map<String, Object> response = new LinkedHashMap<>();
        response.put("concept", "DataFrame: groupBy(department).agg(count, avg, max) ordered by avg_salary");
        response.put("results", dataFrameService.deptSummary());
        return ResponseEntity.ok(response);
    }

    @GetMapping("/dataframe/sales")
    public ResponseEntity<Map<String, Object>> sales() {
        Map<String, Object> response = new LinkedHashMap<>();
        response.put("concept", "DataFrame: read newline-delimited JSON → groupBy two columns → sum + avg");
        response.put("results", dataFrameService.querySales());
        return ResponseEntity.ok(response);
    }

    // ── Dataset endpoints (typed) ─────────────────────────────────────────────

    @GetMapping("/dataset/high-earners")
    public ResponseEntity<Map<String, Object>> highEarners(
            @RequestParam(defaultValue = "80000") double minSalary) {

        List<Employee> employees = datasetService.filterHighEarners(minSalary);
        Map<String, Object> response = new LinkedHashMap<>();
        response.put("concept", "Dataset<Employee>: type-safe FilterFunction — compile-time checked lambda");
        response.put("filter", "salary >= " + minSalary);
        response.put("count", employees.size());
        response.put("employees", employees);
        return ResponseEntity.ok(response);
    }

    @GetMapping("/dataset/summaries")
    public ResponseEntity<Map<String, Object>> summaries() {
        Map<String, Object> response = new LinkedHashMap<>();
        response.put("concept", "Dataset<Employee>.map(MapFunction<Employee, String>): typed transformation");
        response.put("summaries", datasetService.employeeSummaries());
        return ResponseEntity.ok(response);
    }

    @GetMapping("/dataset/highest-paid")
    public ResponseEntity<Map<String, Object>> highestPaid() {
        Map<String, Object> response = new LinkedHashMap<>();
        response.put("concept", "Dataset<Employee>.reduce(ReduceFunction<Employee>): fold to single value");
        response.put("highest_paid", datasetService.highestPaid());
        return ResponseEntity.ok(response);
    }

    @GetMapping("/health")
    public ResponseEntity<Map<String, String>> health() {
        return ResponseEntity.ok(Map.of("status", "ok", "spark_mode", "local[*]"));
    }
}
