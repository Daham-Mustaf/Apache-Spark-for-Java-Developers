package org.daham.chapter_1.util;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

public class SparkUtil {
    private static JavaSparkContext sc;

    public static JavaSparkContext setupSparkContext(SparkConf conf) {
        if (sc == null) {
            sc = new JavaSparkContext(conf);
        }
        return sc;
    }

    public static void closeSparkContext() {
        if (sc != null) {
            sc.stop();
            sc.close();
            sc = null;
        }
    }
}
