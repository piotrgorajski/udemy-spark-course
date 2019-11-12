package org.goraj.sparkcourse;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Map;

public class WordCountBetterSorted {

    public static void main(String[] args) {
        // Create a SparkContext using every core of the local machine
        JavaSparkContext sc = new JavaSparkContext("local", "WordCountBetterSorted");

        // Load each line of the source data into an RDD
        JavaRDD<String> input = sc.textFile("src/main/resources/book.txt");

        // Split into words separated by a space character
        JavaRDD<String> words = input.flatMap(x -> Arrays.asList(x.split("\\W+")).iterator());

        JavaRDD<String> lowercaseWords = words.map(String::toLowerCase);

        // Count up the occurrences of each word
        JavaPairRDD<String, Integer> wordCounts = lowercaseWords.mapToPair(word -> new Tuple2<>(word, 1)).reduceByKey(Integer::sum);

        JavaPairRDD<Integer, String> wordCountsSorted = wordCounts.mapToPair(x -> new Tuple2<>(x._2(), x._1())).sortByKey();

        // Print the results.
        wordCountsSorted.foreach(x -> System.out.printf("%s: %d%n", x._2(), x._1()));
    }
}
