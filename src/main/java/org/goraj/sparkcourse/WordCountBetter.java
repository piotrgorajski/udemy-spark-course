package org.goraj.sparkcourse;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.Map;

public class WordCountBetter {

    public static void main(String[] args) {
        // Create a SparkContext using every core of the local machine
        JavaSparkContext sc = new JavaSparkContext("local[*]", "WordCountBetter");

        // Load each line of the source data into an RDD
        JavaRDD<String> input = sc.textFile("src/main/resources/book.txt");

        // Split into words separated by a space character
        JavaRDD<String> words = input.flatMap(x -> Arrays.asList(x.split("\\W+")).iterator());

        JavaRDD<String> lowercaseWords = words.map(String::toLowerCase);

        // Count up the occurrences of each word
        Map<String, Long> wordCounts = lowercaseWords.countByValue();

        // Print the results.
        wordCounts.forEach((key, value) -> System.out.printf("%s: %d%n", key, value));
    }
}
