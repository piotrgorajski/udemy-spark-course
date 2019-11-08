package org.goraj.sparkcourse;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Comparator;
import java.util.List;

public class FriendsByFirstName {

    public static void main(String[] args) {
        // Create a SparkContext using every core of the local machine
        JavaSparkContext sc = new JavaSparkContext("local[*]", "FriendsByFirstName");

        // Load each line of the source data into an RDD
        JavaRDD<String> lines = sc.textFile("src/main/resources/fakefriends.csv");

        // Use our parseLines function to convert to (age, numFriends) tuples
        JavaPairRDD<String, Integer> rdd = lines.mapToPair(FriendsByFirstName::parseLine);

        // Lots going on here...
        // We are starting with an RDD of form (age, numFriends) where age is the KEY and numFriends is the VALUE
        // We use mapValues to convert each numFriends value to a tuple of (numFriends, 1)
        // Then we use reduceByKey to sum up the total numFriends and total instances for each age, by
        // adding together all the numFriends values and 1's respectively.
        JavaPairRDD<String, Tuple2<Integer, Integer>> totalsByAge = rdd.mapValues(x -> new Tuple2<>(x, 1)).reduceByKey((x, y) -> new Tuple2<>(x._1 + y._1, x._2 + y._2));

        // So now we have tuples of (age, (totalFriends, totalInstances))
        // To compute the average we divide totalFriends / totalInstances for each age.
        JavaPairRDD<String, Integer> averagesByFirstName = totalsByAge.mapValues(x -> x._1 / x._2);

        // Collect the results from the RDD (This kicks off computing the DAG and actually executes the job)
        List<Tuple2<String, Integer>> results = averagesByFirstName.collect();

        // Sort and print the final results.
        results.stream().sorted(Comparator.comparing(Tuple2::_1)).forEach(System.out::println);
    }

    /**
     * A function that splits a line of input into (age, numFriends) tuples.
     */
    private static Tuple2<String, Integer> parseLine(String line) {
        // Split by commas
        String[] fields = line.split(",");
        // Extract the age and numFriends fields, and convert to integers
        String firstName = fields[1];
        Integer numFriends = Integer.valueOf(fields[3]);
        // Create a tuple that is our result.
        return new Tuple2<>(firstName, numFriends);
    }
}
