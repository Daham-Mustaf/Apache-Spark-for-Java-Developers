package org.daham.chapter_1;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.daham.chapter_1.util.SparkUtil;

public class SparkJob {
    public static void main(String[] args) {
        // Set up the SparkConf with desired configuration settings
        SparkConf conf = new SparkConf()
                .setAppName("My Spark Job")
                .setMaster("local");

        // Call the static setupSparkContext() method to get the initialized SparkContext
        JavaSparkContext sc = SparkUtil.setupSparkContext(conf);

        // Your Spark operations here...
        // For example, read a CSV file and perform some transformations
        // For this example, we will just print the Spark version
        System.out.println("Spark Version: " + sc.version());

        // Call the static closeSparkContext() method to stop and close the SparkContext when done
        SparkUtil.closeSparkContext();
    }
}
