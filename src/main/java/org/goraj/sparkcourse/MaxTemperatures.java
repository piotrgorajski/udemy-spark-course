package org.goraj.sparkcourse;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import scala.Tuple3;

import java.util.List;

public class MaxTemperatures {

    public static void main(String[] args) {
        // Create a SparkContext using every core of the local machine
        JavaSparkContext sc = new JavaSparkContext("local[*]", "MaxTemperatures");

        // Load each line of the source data into an RDD
        JavaRDD<String> lines = sc.textFile("src/main/resources/1800.csv");

        // Convert to (stationID, entryType, temperature) tuples
        JavaRDD<Tuple3<String, String, Double>> parsedLines = lines.map(MaxTemperatures::parseLine);

        // Filter out all but TMIN entries
        JavaRDD<Tuple3<String, String, Double>> maxTemps = parsedLines.filter(x -> x._2().equals("TMAX"));

        // Convert to (stationID, temperature)
        JavaPairRDD<String, Double> stationTemps = maxTemps.mapToPair(x -> new Tuple2<>(x._1(), x._3()));

        // Reduce by stationID retaining the minimum temperature found
        JavaPairRDD<String, Double> minTempsByStation = stationTemps.reduceByKey(Math::max);

        // Collect, format, and print the results
        List<Tuple2<String, Double>> results = minTempsByStation.collect();

        results.forEach(result -> System.out.printf("%s maximum temperature: %.2f C%n", result._1, result._2));
    }

    private static Tuple3<String, String, Double> parseLine(String line) {
        String[] fields = line.split(",");
        String stationID = fields[0];
        String entryType = fields[2];
        double temperature = Double.parseDouble(fields[3]) * 0.1D;
        return new Tuple3<>(stationID, entryType, temperature);
    }
}
