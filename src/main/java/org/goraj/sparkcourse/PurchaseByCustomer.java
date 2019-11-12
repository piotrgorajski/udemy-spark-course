package org.goraj.sparkcourse;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

public class PurchaseByCustomer {

    public static void main(String[] args) {
        JavaSparkContext sc = new JavaSparkContext("local[*]", "PurchaseByCustomer");

        JavaRDD<String> input = sc.textFile("src/main/resources/customer-orders.csv");

        JavaPairRDD<Integer, Double> purchases = input.mapToPair(PurchaseByCustomer::parseLine);

        JavaPairRDD<Double, Integer> customerPurchases = purchases.reduceByKey(Double::sum).mapToPair(x -> new Tuple2<>(x._2(), x._1())).sortByKey();

        customerPurchases.foreach(x -> System.out.printf("%s: %.2f%n", x._2(), x._1()));
    }

    private static Tuple2<Integer, Double> parseLine(String line) {
        String[] fields = line.split(",");
        Integer customerId = Integer.valueOf(fields[0]);
        Double amount = Double.valueOf(fields[2]);
        return new Tuple2<>(customerId, amount);
    }
}
