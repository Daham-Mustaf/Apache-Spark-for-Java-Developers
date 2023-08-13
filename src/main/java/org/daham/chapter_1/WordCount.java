package org.daham.chapter_1;


import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.nio.file.Path;

public class WordCount {
    private final String inputFile;
    private final String outputFile;

    public WordCount(String inputFile, String outputFile) {
        this.inputFile = inputFile;
        this.outputFile = outputFile;
    }

    private JavaSparkContext setupSparkContext() {
        SparkConf conf = new SparkConf().setAppName("WordCount").setMaster("local");
        return new JavaSparkContext(conf);
    }

    private void closeSparkContext(JavaSparkContext sc) {
        sc.stop();
        sc.close();
    }

    private void deleteOutputDirectory(String outputDir) {
        try {
            org.apache.hadoop.fs.FileSystem fs = org.apache.hadoop.fs.FileSystem.get(new org.apache.hadoop.conf.Configuration());
            fs.delete(new org.apache.hadoop.fs.Path(outputDir), true);
            System.out.println("\nOutput directory '" + outputDir + "' has been deleted successfully.\n");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void runWordCount() {
        JavaSparkContext sc = setupSparkContext();

        // Delete the output directory if it already exists
        deleteOutputDirectory(outputFile);

        JavaRDD<String> lines = sc.textFile(inputFile);

        JavaPairRDD<String, Integer> wordCounts = lines
                .flatMap(line -> Arrays.asList(line.split(" ")).iterator())
                .mapToPair(word -> new Tuple2<>(word, 1))
                .reduceByKey(Integer::sum);

        wordCounts.saveAsTextFile(outputFile);
        closeSparkContext(sc);
    }
    public void readWholeDirectory(){
        final String testDirPath = Path.of("src",  "main", "resources").toString();
        JavaSparkContext sc = setupSparkContext();
        final var myRdd = sc.wholeTextFiles(testDirPath);
        System.out.printf("Total number of files in directory %s = %d%n", testDirPath, myRdd.count());
    }
    public void loadingCSVFileIntoSparkRDD(){
        try (JavaSparkContext sc = setupSparkContext()) {
            final String csvFilePath = Path.of("src", "main", "resources", "dma.csv").toString();
            JavaRDD<String> myRdd = sc.textFile(csvFilePath);
            System.out.printf("Total lines in file: %d%n", myRdd.count());

            System.out.println("CSV Headers~>");
            System.out.println(myRdd.first());
            System.out.println("--------------------");
            System.out.println("\nPrint first 10 lines.....");
            myRdd.take(10).forEach(System.out::println);
            System.out.println("--------------------");

            System.out.println("\n--------------------");
            System.out.println("join fields with |");
            JavaRDD<String[]> cvsFields = myRdd.map(line -> line.split(","));
            cvsFields.take(5).forEach(field-> System.out.println(String.join("|", field)));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }



    public static void main(String[] args) {
        String inputFile = "/Users/daham/Desktop/Shacl/apache-java-spark/src/main/resources/1000words.txt";
        String outputFile = "/Users/daham/Desktop/Shacl/apache-java-spark/src/main/WORDCOUNTER";

        WordCount wordCount = new WordCount(inputFile, outputFile);
//        wordCount.runWordCount();
//        wordCount.readWholeDirectory();
        final String testCSVFilePath = Path.of("src", "main", "resources", "dma.csv").toString();
//        System.out.println(testCSVFilePath);
        wordCount.loadingCSVFileIntoSparkRDD();
    }
}
