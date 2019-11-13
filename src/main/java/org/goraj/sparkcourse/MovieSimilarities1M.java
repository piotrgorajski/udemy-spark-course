package org.goraj.sparkcourse;

import lombok.Value;
import org.apache.commons.lang3.builder.CompareToBuilder;
import org.apache.spark.HashPartitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.io.IOException;
import java.io.Serializable;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static java.lang.Math.sqrt;

public class MovieSimilarities1M {

    /**
     * Load up a Map of movie IDs to movie names.
     */
    private static Map<Integer, String> loadMovieNames() throws IOException {
        return Files.readAllLines(Paths.get("movies.dat"), Charset.forName("windows-1250"))
                .stream()
                .map(line -> line.split("::"))
                .filter(fields -> fields.length > 1)
                .collect(Collectors.toMap(fields -> Integer.valueOf(fields[0]), fields -> fields[1]));
    }

    @Value
    private static final class MovieRating implements Serializable {
        private int movieId;
        private double rating;
    }

    @Value
    private static final class SimilarityResult implements Serializable, Comparable {
        private double score;
        private int pairCount;

        @Override
        public int compareTo(Object o) {
            return CompareToBuilder.reflectionCompare(this, o);
        }
    }

    @Value
    private static final class UserRatingPair implements Serializable {
        private int userId;
        private MovieRating left;
        private MovieRating right;

    }

    @Value
    private static final class RatingPair implements Serializable {
        private double left;
        private double right;
    }

    private static final class RatingPairs extends ArrayList<RatingPair> {
    }

    private static Tuple2<Tuple2<Integer, Integer>, RatingPair> makePairs(UserRatingPair userRatings) {
        MovieRating movieRating1 = userRatings.left;
        MovieRating movieRating2 = userRatings.right;

        int movie1 = movieRating1.movieId;
        double rating1 = movieRating1.rating;
        int movie2 = movieRating2.movieId;
        double rating2 = movieRating2.rating;

        return new Tuple2<>(new Tuple2<>(movie1, movie2), new RatingPair(rating1, rating2));
    }

    private static boolean filterDuplicates(UserRatingPair userRatings) {
        MovieRating movieRating1 = userRatings.left;
        MovieRating movieRating2 = userRatings.right;

        int movie1 = movieRating1.movieId;
        int movie2 = movieRating2.movieId;

        return movie1 < movie2;
    }


    private static Tuple2<Double, Integer> computeCosineSimilarity(Iterable<RatingPair> ratingPairs) {
        int numPairs = 0;
        double sum_xx = 0.0D;
        double sum_yy = 0.0D;
        double sum_xy = 0.0D;

        for (RatingPair pair : ratingPairs) {
            double ratingX = pair.left;
            double ratingY = pair.right;

            sum_xx += ratingX * ratingX;
            sum_yy += ratingY * ratingY;
            sum_xy += ratingX * ratingY;
            numPairs += 1;
        }

        double numerator = sum_xy;
        double denominator = sqrt(sum_xx) * sqrt(sum_yy);

        double score = 0.0D;
        if (denominator != 0) {
            score = numerator / denominator;
        }

        return new Tuple2<>(score, numPairs);
    }

    public static void main(String[] args) throws IOException {
        SparkConf conf = new SparkConf();
        conf.setAppName("MovieSimilarities1M");
        JavaSparkContext sc = new JavaSparkContext(conf);

        System.out.println("Loading movie names...");
        Map<Integer, String> nameDict = loadMovieNames();

        JavaRDD<String> data = sc.textFile("s3n://sundog-spark/ml-1m/ratings.dat");

        // Map ratings to key / value pairs: user ID => movie ID, rating
        JavaPairRDD<Integer, MovieRating> ratings = data
                .map(line -> line.split("::"))
                .mapToPair(line -> new Tuple2<>(Integer.valueOf(line[0]), new MovieRating(Integer.parseInt(line[1]), Double.parseDouble(line[2]))));

        // Emit every movie rated together by the same user.
        // Self-join to find every combination.
        JavaPairRDD<Integer, Tuple2<MovieRating, MovieRating>> joinedRatings = ratings.join(ratings);

        // At this point our RDD consists of userID => ((movieID, rating), (movieID, rating))

        // Filter out duplicate pairs
        JavaRDD<UserRatingPair> uniqueJoinedRatings = joinedRatings.map(d -> new UserRatingPair(d._1(), d._2()._1(), d._2()._2())).filter(MovieSimilarities1M::filterDuplicates);

        // Now key by (movie1, movie2) pairs.
        JavaPairRDD<Tuple2<Integer, Integer>, RatingPair> moviePairs = uniqueJoinedRatings.mapToPair(MovieSimilarities1M::makePairs).partitionBy(new HashPartitioner(100));

        // We now have (movie1, movie2) => (rating1, rating2)
        // Now collect all ratings for each movie pair and compute similarity
        JavaPairRDD<Tuple2<Integer, Integer>, Iterable<RatingPair>> moviePairRatings = moviePairs.groupByKey();

        // We now have (movie1, movie2) = > (rating1, rating2), (rating1, rating2) ...
        // Can now compute similarities.
        JavaPairRDD<Tuple2<Integer, Integer>, Tuple2<Double, Integer>> moviePairSimilarities = moviePairRatings.mapValues(MovieSimilarities1M::computeCosineSimilarity).cache();

        //Save the results if desired
        //val sorted = moviePairSimilarities.sortByKey()
        //sorted.saveAsTextFile("movie-sims")

        // Extract similarities for the movie we care about that are "good".

        if (args.length > 0) {
            double scoreThreshold = 0.97D;
            double coOccurenceThreshold = 50.0;

            int movieID = Integer.parseInt(args[0]);

            // Filter for movies with this sim that are "good" as defined by
            // our quality thresholds above

            JavaPairRDD<Tuple2<Integer, Integer>, Tuple2<Double, Integer>> filteredResults = moviePairSimilarities.filter(x ->
                    {
                        Tuple2<Integer, Integer> pair = x._1;
                        Tuple2<Double, Integer> sim = x._2;
                        return (pair._1 == movieID || pair._2 == movieID) && sim._1 > scoreThreshold && sim._2 > coOccurenceThreshold;
                    }
            );

            // Sort by quality score.
            //FIXME wrap double int tuple
            List<Tuple2<SimilarityResult, Tuple2<Integer, Integer>>> results = filteredResults.mapToPair(x -> new Tuple2<>(new SimilarityResult(x._2._1(), x._2._2()), x._1)).sortByKey(false).take(10);

            System.out.printf("%nTop 10 similar movies for %s%n", nameDict.get(movieID));
            results.forEach(result -> {
                SimilarityResult sim = result._1;
                Tuple2<Integer, Integer> pair = result._2;
                // Display the similarity result that isn't the movie we're looking at
                Integer similarMovieID = pair._1;
                if (similarMovieID == movieID) {
                    similarMovieID = pair._2;
                }
                System.out.println(nameDict.get(similarMovieID) + "\tscore: " + sim.score + "\tstrength: " + sim.pairCount);
            });
        }
    }
}
