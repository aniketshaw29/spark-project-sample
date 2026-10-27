package com.example.spark.config;

import org.apache.spark.sql.SparkSession;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class SparkConfig {

    @Bean
    public SparkSession sparkSession() {
        return SparkSession.builder()
                .appName("SpringBootSpark")
                // local[*] = run locally using all available CPU cores (no cluster needed)
                .master("local[*]")
                // disable the Spark web UI (port 4040) to avoid conflicts in dev
                .config("spark.ui.enabled", "false")
                .config("spark.driver.bindAddress", "127.0.0.1")
                // keep shuffle partitions low for local dev; default is 200 (way too many locally)
                .config("spark.sql.shuffle.partitions", "4")
                .getOrCreate();
    }
}
