package org.goraj.sparkcourse;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Map;
import java.util.stream.Collectors;

public class PopularMoviesNicer {

    public static void main(String[] args) throws IOException {
        JavaSparkContext sc = new JavaSparkContext("local[*]", "PopularMoviesNicer");

        Broadcast<Map<Integer, String>> nameDict = sc.broadcast(loadMovieNames());

        JavaRDD<String> lines = sc.textFile("src/main/resources/u.data", 1);

        JavaPairRDD<Integer, Integer> movies = lines.mapToPair(x -> new Tuple2<>(Integer.valueOf(x.split("\t")[1]), 1));

        JavaPairRDD<Integer, Integer> moviesCounts = movies.reduceByKey(Integer::sum);

        JavaPairRDD<Integer, Integer> sortedMovieCounts = moviesCounts.mapToPair(x -> new Tuple2<>(x._2(), x._1())).sortByKey();

        JavaPairRDD<String, Integer> sortedMoviesWithNames = sortedMovieCounts.mapToPair(x -> new Tuple2<>(nameDict.value().get(x._2()), x._1()));

        sortedMoviesWithNames.foreach(x -> System.out.printf("%s: %d%n", x._1(), x._2()));
    }

    /**
     * Load up a Map of movie IDs to movie names.
     */
    private static Map<Integer, String> loadMovieNames() throws IOException {
        return Files.readAllLines(Paths.get("src/main/resources/u.item"), Charset.forName("windows-1250"))
                .stream()
                .map(line -> line.split("\\|"))
                .filter(fields -> fields.length > 1)
                .collect(Collectors.toMap(fields -> Integer.valueOf(fields[0]), fields -> fields[1]));
    }
}
