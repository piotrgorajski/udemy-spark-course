package org.goraj.sparkcourse;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Comparator;
import java.util.List;

public class FriendsByAge {

    public static void main(String[] args) {
        // Create a SparkContext using every core of the local machine
        JavaSparkContext sc = new JavaSparkContext("local[*]", "FriendsByAge");

        // Load each line of the source data into an RDD
        JavaRDD<String> lines = sc.textFile("src/main/resources/fakefriends.csv");

        // Use our parseLines function to convert to (age, numFriends) tuples
        JavaPairRDD<Integer, Integer> rdd = lines.mapToPair(FriendsByAge::parseLine);

        // Lots going on here...
        // We are starting with an RDD of form (age, numFriends) where age is the KEY and numFriends is the VALUE
        // We use mapValues to convert each numFriends value to a tuple of (numFriends, 1)
        // Then we use reduceByKey to sum up the total numFriends and total instances for each age, by
        // adding together all the numFriends values and 1's respectively.
        JavaPairRDD<Integer, Tuple2<Integer, Integer>> totalsByAge = rdd.mapValues(x -> new Tuple2<>(x, 1)).reduceByKey((x, y) -> new Tuple2<>(x._1 + y._1, x._2 + y._2));

        // So now we have tuples of (age, (totalFriends, totalInstances))
        // To compute the average we divide totalFriends / totalInstances for each age.
        JavaPairRDD<Integer, Integer> averagesByAge = totalsByAge.mapValues(x -> x._1 / x._2);

        // Collect the results from the RDD (This kicks off computing the DAG and actually executes the job)
        List<Tuple2<Integer, Integer>> results = averagesByAge.collect();

        // Sort and print the final results.
        results.stream().sorted(Comparator.comparingInt(Tuple2::_1)).forEach(System.out::println);
    }

    /**
     * A function that splits a line of input into (age, numFriends) tuples.
     */
    private static Tuple2<Integer, Integer> parseLine(String line) {
        // Split by commas
        String[] fields = line.split(",");
        // Extract the age and numFriends fields, and convert to integers
        Integer age = Integer.valueOf(fields[2]);
        Integer numFriends = Integer.valueOf(fields[3]);
        // Create a tuple that is our result.
        return new Tuple2<>(age, numFriends);
    }
}
