package org.goraj.sparkcourse.sql;

import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;

public class DataFrames {

    private static MapFunction<String, Person> lineToPerson = value -> {
        String[] fields = value.split(",");
        return new Person(Integer.parseInt(fields[0]), fields[1], Integer.parseInt(fields[2]), Integer.parseInt(fields[3]));
    };

    /**
     * Our main function where the action happens
     */
    public static void main(String[] args) {
        // Use new SparkSession interface in Spark 2.0
        try (SparkSession sparkSession = SparkSession
                .builder()
                .appName("SparkSQL")
                .master("local[*]")
                .getOrCreate()) {

            Encoder<Person> personEncoder = Encoders.bean(Person.class);
            Dataset<Person> people = sparkSession.sqlContext().read().textFile("src/main/resources/fakefriends.csv").map(lineToPerson, personEncoder).cache();

            System.out.println("Here is our inferred schema:");
            people.printSchema();

            System.out.println("Let's select the name column:");
            people.select("name").show();

            System.out.println("Filter out anyone over 21:");
            people.filter(people.col("age").lt(21)).show();

            System.out.println("Group by age:");
            people.groupBy("age").count().show();

            System.out.println("Make everyone 10 years older:");
            people.select(people.col("name"), people.col("age").plus(10)).show();
        }
    }
}
