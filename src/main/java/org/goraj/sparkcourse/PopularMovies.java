package org.goraj.sparkcourse;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

public class PopularMovies {

    public static void main(String[] args) {
        // Create a SparkContext using every core of the local machine, named RatingsCounter
        JavaSparkContext sc = new JavaSparkContext("local[*]", "PopularMovies");

        JavaRDD<String> lines = sc.textFile("src/main/resources/u.data", 1);

        JavaPairRDD<String, Integer> movies = lines.mapToPair(x -> new Tuple2<>(x.split("\t")[1], 1));

        JavaPairRDD<String, Integer> moviesCounts = movies.reduceByKey(Integer::sum);

        JavaPairRDD<Integer, String> sortedMovieCounts = moviesCounts.mapToPair(x -> new Tuple2<>(x._2(), x._1())).sortByKey();

        sortedMovieCounts.foreach(x -> System.out.printf("%s: %d%n", x._2(), x._1()));
    }
}
