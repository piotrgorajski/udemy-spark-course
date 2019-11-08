package org.goraj.sparkcourse;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

public class RatingsCounter {

    public static void main(String[] args) {
        // Create a SparkContext using every core of the local machine, named RatingsCounter
        JavaSparkContext sc = new JavaSparkContext("local[*]", "RatingsCounter");

        // Load up each line of the ratings data into an RDD
        JavaRDD<String> lines = sc.textFile("src/main/resources/u.data", 1);

        // Convert each line to a string, split it out by tabs, and extract the third field.
        // (The file format is userID, movieID, rating, timestamp)
        JavaRDD<String> ratings = lines.map(x -> x.split("\t")[2]);

        // Count up how many times each value (rating) occurs
        Map<String, Long> results = ratings.countByValue();

        // Sort the resulting map of (rating, count) tuples
        SortedMap<String, Long> sortedResults = new TreeMap<>(results);

        // Print each result on its own line.
        sortedResults.forEach((key, value) -> System.out.printf("%s: %d%n", key, value));
    }
}
