package org.daham.chapter_1;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

import java.util.List;
import java.util.Scanner;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class SparkProgramming {
    public static void main(String[] args) {
        try (var spark = SparkSession.builder()
                .appName("SparkProgramming")
                .master("local[*]")
                .getOrCreate();
             final var sc = new JavaSparkContext(spark.sparkContext())) {

            final var inputData = Stream.iterate(1, n -> n + 1).limit(5).collect(Collectors.toList());
            final var rdd = sc.parallelize(inputData);
            printRDD(rdd);
            printRDDCount(rdd);
            printNumPartitions(rdd);
            printMaxMinSum(rdd);
            // Wait for user input to terminate the program
            waitForTermination();


        } catch (Exception e) {
            // Exception handling for Spark
            e.printStackTrace();
            // Additional error handling logic
        }
    }

    private static void printRDDCount(org.apache.spark.api.java.JavaRDD<Integer> rdd) {
        final long count = rdd.count();
        System.out.printf("Total number of RDD elements: %d%n", count);
    }

    private static void printNumPartitions(org.apache.spark.api.java.JavaRDD<Integer> rdd) {
        final int numPartitions = rdd.getNumPartitions();
        System.out.printf("Total number of partitions: %d%n", numPartitions);
    }

    private static void printMaxMinSum(org.apache.spark.api.java.JavaRDD<Integer> rdd) {
        final int max = rdd.reduce(Integer::max);
        final int min = rdd.reduce(Integer::min);
        final int sum = rdd.reduce(Integer::sum);
        System.out.printf("MAX-> %d, MIN-> %d, SUM-> %d%n", max, min, sum);
    }
    private static void printRDD(org.apache.spark.api.java.JavaRDD<Integer> rdd) {
        List<Integer> rddElements = rdd.collect();
        for (Integer element : rddElements) {
            System.out.println(element);
        }
    }
    private static void waitForTermination() {
        System.out.println("Press Enter to terminate the program...");
        try (Scanner scanner = new Scanner(System.in)) {
            scanner.nextLine();
        }
    }


}
