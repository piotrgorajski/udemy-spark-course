package org.goraj.sparkcourse;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.io.IOException;
import java.io.Serializable;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;

public class MostPopularSuperhero {

    public static void main(String[] args) throws IOException {
        JavaSparkContext sc = new JavaSparkContext("local[*]", "MostPopularSuperhero");

        // Build up a hero ID -> name RDD
        JavaRDD<String> names = sc.textFile("src/main/resources/marvel-names.txt");
        JavaPairRDD<Integer, String> namesRdd = names.flatMapToPair(MostPopularSuperhero::parseNames);

        // Load up the superhero co-apperarance data
        JavaRDD<String> lines = sc.textFile("src/main/resources/marvel-graph.txt");

        // Convert to (heroID, number of connections) RDD
        JavaPairRDD<Integer, Integer> pairings = lines.mapToPair(MostPopularSuperhero::countCoOccurrences);

        // Combine entries that span more than one line
        JavaPairRDD<Integer, Integer> totalFriendsByCharacter = pairings.reduceByKey(Integer::sum);

        // Find the max # of connections
        Tuple2<Integer, Integer> mostPopular = totalFriendsByCharacter.max(new TupleComparator());

        // Look up the name (lookup returns an array of results, so we need to access the first result with (0)).
        String mostPopularName = namesRdd.lookup(mostPopular._1()).get(0);

        // Print out our answer!
        System.out.printf("%s is the most popular superhero with %d co-appearances.%n", mostPopularName, mostPopular._2());
    }

    // Function to extract the hero ID and number of connections from each line
    private static Tuple2<Integer, Integer> countCoOccurrences(String line) {
        String[] elements = line.split("\\s+");
        return new Tuple2<>(Integer.valueOf(elements[0]), elements.length - 1);
    }

    // Function to extract hero ID -> hero name tuples (or None in case of failure)
    private static Iterator<Tuple2<Integer, String>> parseNames(String line) {
        String[] fields = line.split("\"");
        if (fields.length > 1) {
            return Collections.singleton(new Tuple2<>(Integer.valueOf(fields[0].trim()), fields[1])).iterator();
        } else {
            return Collections.emptyIterator(); // flatmap will just discard None results, and extract data from Some results.
        }
    }
}

class TupleComparator implements Comparator<Tuple2<Integer, Integer>>, Serializable {
    @Override
    public int compare(Tuple2<Integer, Integer> o1, Tuple2<Integer, Integer> o2) {
        return o1._2() - o2._2();
    }
}
