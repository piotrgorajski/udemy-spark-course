package org.goraj.sparkcourse.streaming;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.streaming.Seconds;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.twitter.TwitterUtils;
import scala.Tuple2;
import twitter4j.Status;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;

public class PopularHashtags {

    public static void main(String[] args) throws IOException, InterruptedException {
        // Configure Twitter credentials using twitter.txt
        setupTwitter();
        Logger.getRootLogger().setLevel(Level.ERROR);

        // Set up a Spark streaming context named "PopularHashtags" that runs locally using
        // all CPU cores and one-second batches of data
        JavaStreamingContext ssc = new JavaStreamingContext("local[*]", "PopularHashtags", Seconds.apply(1L));

        // Create a DStream from Twitter using our streaming context
        JavaReceiverInputDStream<Status> tweets = TwitterUtils.createStream(ssc);

        // Now extract the text of each status update into DStreams using map()
        JavaDStream<String> statuses = tweets.map(Status::getText);

        // Blow out each word into a new DStream
        JavaDStream<String> tweetwords = statuses.flatMap(tweetText -> Arrays.asList(tweetText.split(" ")).iterator());

        // Now eliminate anything that's not a hashtag
        JavaDStream<String> hashtags = tweetwords.filter(word -> word.startsWith("#"));

        // Map each hashtag to a key/value pair of (hashtag, 1) so we can count them up by adding up the values
        JavaPairDStream<String, Integer> hashtagKeyValues = hashtags.mapToPair(hashtag -> new Tuple2<>(hashtag, 1));

        // Now count them up over a 5 minute window sliding every one second
        JavaPairDStream<String, Integer> hashtagCounts = hashtagKeyValues.reduceByKeyAndWindow(Integer::sum, (x, y) -> x - y, Seconds.apply(300), Seconds.apply(1));
        //  You will often see this written in the following shorthand:
        //val hashtagCounts = hashtagKeyValues.reduceByKeyAndWindow( _ + _, _ -_, Seconds(300), Seconds(1))

        // Sort the results by the count values
        JavaPairDStream<Integer, String> sortedResults = hashtagCounts.transformToPair(rdd -> rdd.mapToPair(x -> new Tuple2<>(x._2(), x._1())).sortByKey(false));

        // Print the top 10
        sortedResults.print();

        // Set a checkpoint directory, and kick it all off
        // I could watch this all day!
        ssc.checkpoint("C:/checkpoint/");
        ssc.start();
        ssc.awaitTermination();
    }

    /**
     * Configures Twitter service credentials using twiter.txt in the main workspace directory
     */
    private static void setupTwitter() throws IOException {
        Files.readAllLines(Paths.get("src/main/resources/twitter.txt"), Charset.forName("windows-1250")).forEach(line -> {
            String[] fields = line.split(" ");
            if (fields.length == 2) {
                System.setProperty("twitter4j.oauth." + fields[0], fields[1]);
            }
        });
    }
}
