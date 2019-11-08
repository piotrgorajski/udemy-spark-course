package org.goraj.sparkcourse;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import scala.Tuple4;

import java.util.List;

public class MaxPrecipitationDate {

    public static void main(String[] args) {
        // Create a SparkContext using every core of the local machine
        JavaSparkContext sc = new JavaSparkContext("local[*]", "MaxTemperatures");

        // Load each line of the source data into an RDD
        JavaRDD<String> lines = sc.textFile("src/main/resources/1800.csv");

        // Convert to (stationID, entryType, temperature) tuples
        JavaRDD<Tuple4<String, String, String, Double>> parsedLines = lines.map(MaxPrecipitationDate::parseLine);

        // Filter out all but TMIN entries
        JavaRDD<Tuple4<String, String, String, Double>> maxTemps = parsedLines.filter(x -> x._3().equals("PRCP"));

        // Convert to (stationID, temperature)
        JavaPairRDD<String, Tuple2<String, Double>> stationTemps = maxTemps.mapToPair(x -> new Tuple2<>(x._1(), new Tuple2<>(x._2(), x._4())));

        // Reduce by stationID retaining the minimum temperature found
        JavaPairRDD<String, Tuple2<String, Double>> maxPrecipitationByStationAndDate = stationTemps.reduceByKey((x, y) -> new Tuple2<>(x._2 > y._2 ? x._1 : y._1, Math.max(x._2, y._2)));

        // Collect, format, and print the results
        List<Tuple2<String, String>> results = maxPrecipitationByStationAndDate.mapToPair(x -> new Tuple2<>(x._1, x._2._1)).collect();

        results.forEach(result -> System.out.printf("%s maximum precipitation was at: %s%n", result._1, result._2));
    }

    private static Tuple4<String, String, String, Double> parseLine(String line) {
        String[] fields = line.split(",");
        String stationID = fields[0];
        String date = fields[1];
        String entryType = fields[2];
        double precipitation = Double.parseDouble(fields[3]);
        return new Tuple4<>(stationID, date, entryType, precipitation);
    }
}
