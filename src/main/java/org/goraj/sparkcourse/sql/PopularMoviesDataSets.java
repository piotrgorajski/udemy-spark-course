package org.goraj.sparkcourse.sql;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.spark.sql.functions.desc;

public class PopularMoviesDataSets {

    public static void main(String[] args) throws IOException {
        try (SparkSession sparkSession = SparkSession
                .builder()
                .appName("PopularMovies")
                .master("local[*]")
                .getOrCreate()) {

            Encoder<Movie> movieEncoder = Encoders.bean(Movie.class);
            MapFunction<String, Movie> lineToMovie = line -> new Movie(Integer.parseInt(line.split("\t")[1]));
            Dataset<Movie> movies = sparkSession.sqlContext().read().textFile("src/main/resources/u.data").map(lineToMovie, movieEncoder);

            // Some SQL-style magic to sort all movies by popularity in one line!
            Dataset<Row> topMovieIDs = movies.groupBy("movieID").count().orderBy(desc("count")).cache();

            // Show the results at this point:
            topMovieIDs.show();

            // Grab the top 10
            List<Row> top10 = topMovieIDs.takeAsList(10);

            // Load up the movie ID -> name map
            Map<Integer, String> names = loadMovieNames();

            // Print the results
            top10.forEach(movie -> System.out.printf("%s: %d%n", names.get(movie.getInt(0)), movie.getLong(1)));
        }
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
