package com.example.spark.service;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * RDD (Resilient Distributed Dataset) — the lowest-level Spark API.
 *
 * Key concepts demonstrated:
 *   - parallelize()  : distribute a Java collection across partitions
 *   - map()          : transform each element (1 → 1)
 *   - flatMap()      : transform each element into multiple (1 → N)
 *   - filter()       : keep elements matching a predicate
 *   - reduce()       : fold all elements into one value (action)
 *   - count()        : count elements (action)
 *   - countByValue() : frequency map (action)
 *   - take(n)        : return first n elements (action)
 *
 * Note: Modern Spark prefers DataFrames/Datasets for structured data because Spark
 * can optimise the query plan. RDDs are useful when data is unstructured or you
 * need fine-grained control over partitioning.
 */
@Service
public class RddService {

    private final SparkSession spark;

    public RddService(SparkSession spark) {
        this.spark = spark;
    }

    // Example 1: Classic word count
    public Map<String, Long> wordCount(String text) {
        // JavaSparkContext wraps the underlying SparkContext for Java-friendly lambdas.
        // We wrap spark.sparkContext() — we are NOT creating a new context (only one
        // SparkContext is allowed per JVM).
        JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());

        JavaRDD<String> words = sc.parallelize(Arrays.asList(text.split("\\s+")))
                .map(String::toLowerCase)
                .filter(w -> w.matches("[a-z]+"));  // drop punctuation

        // countByValue() is an action — it triggers the computation and returns a Map
        Map<String, Long> counts = words.countByValue();

        // sort by frequency, return top 10
        return counts.entrySet().stream()
                .sorted(Map.Entry.<String, Long>comparingByValue().reversed())
                .limit(10)
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        Map.Entry::getValue,
                        (a, b) -> a,
                        LinkedHashMap::new
                ));
    }

    // Example 2: Numeric RDD — map / filter / reduce
    public Map<String, Object> numberOps() {
        JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());

        List<Integer> numbers = IntStream.rangeClosed(1, 100).boxed().collect(Collectors.toList());
        JavaRDD<Integer> rdd = sc.parallelize(numbers);

        // Spark evaluates these lazily — nothing runs until an action (reduce/count) is called
        long sum          = rdd.reduce(Integer::sum);
        long evenCount    = rdd.filter(n -> n % 2 == 0).count();
        long sumOfSquares = rdd.filter(n -> n % 2 == 0)
                               .map(n -> n * n)
                               .reduce(Integer::sum);

        Map<String, Object> result = new LinkedHashMap<>();
        result.put("input", "numbers 1 to 100");
        result.put("sum", sum);
        result.put("even_count", evenCount);
        result.put("sum_of_even_squares", sumOfSquares);
        return result;
    }

    // Example 3: flatMap — one element produces many
    public Map<String, Object> flatMapExample() {
        JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());

        List<String> sentences = Arrays.asList(
                "Apache Spark is fast",
                "Spark processes big data",
                "Java and Spark work well together"
        );

        JavaRDD<String> wordsRdd = sc.parallelize(sentences)
                // flatMap: each String → Iterator<String> (sentence → words)
                .flatMap(sentence -> Arrays.asList(sentence.split(" ")).iterator());

        Map<String, Object> result = new LinkedHashMap<>();
        result.put("input_sentences", sentences.size());
        result.put("total_words", wordsRdd.count());
        result.put("distinct_words", wordsRdd.distinct().count());
        result.put("sample_words", wordsRdd.take(5));
        return result;
    }
}
