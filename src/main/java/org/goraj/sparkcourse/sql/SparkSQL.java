package org.goraj.sparkcourse.sql;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;

import java.util.List;

public class SparkSQL {

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
            Dataset<Person> schemaPeople = sparkSession.sqlContext().read().textFile("src/main/resources/fakefriends.csv").map(lineToPerson, personEncoder);

            schemaPeople.printSchema();

            schemaPeople.createOrReplaceTempView("people");

            // SQL can be run over DataFrames that have been registered as a table
            Dataset<Person> teenagers = sparkSession.sql("SELECT * FROM people WHERE age >= 13 AND age <= 19").as(personEncoder);

            List<Person> results = teenagers.collectAsList();

            results.forEach(System.out::println);
        }
    }
}
