package com.sparkling_taxi;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDDLike;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;
import scala.Tuple3;

import java.io.Serializable;
import java.util.Comparator;

public class Query3 {
    /***
     * Identify the top-5 most popular DOLocationIDs, indicating for each one:
     * - the average number of passengers,
     * - the mean and standard deviation of fare_amount
     * @param args
     */
    public static final String FILE_Q3 = "hdfs://namenode:9000/home/dataset-batch/Query3.parquet";
    public static final int DO_LOC_COL = 3;
    public static final int PASSENGER_COUNT_COL = 2;
    public static final int FARE_AMOUNT_COL = 5;

    public static void main(String[] args) {
        try (SparkSession spark = SparkSession
                .builder()
                .master("spark://spark:7077")
                .appName("Query3")
                .getOrCreate()
        ) {
            spark.sparkContext().setLogLevel("WARN");
            Performance.measure("Query3 - file4", () -> mostPopularDestination(spark, FILE_Q3));
        }
    }

    private static void mostPopularDestination(SparkSession spark, String file) {
        // (DOLocationID, 1, passeggeri, fare_amount)
        // reduceByKey() -> (DOLocation, numero_corse, somma_passeggeri, somma_fare_amount)
        // reduceByKey((x, y) -> (x >= y)) -> ranking di (DOLocation, numero_corse)
        // map(tup -> new (tup._1, tup._2, tup._3 / tup._2, tup._4 / tup._2)

        JavaPairRDD<Long, Tuple3<Integer, Double, Double>> rdd = spark.read().parquet(file)
                .toJavaRDD()
                .filter(row -> !(row.isNullAt(DO_LOC_COL) || row.isNullAt(PASSENGER_COUNT_COL) || row.isNullAt(FARE_AMOUNT_COL)))
                .mapToPair(row -> new Tuple2<>(row.getLong(DO_LOC_COL), new Tuple3<>(1, row.getDouble(PASSENGER_COUNT_COL), row.getDouble(FARE_AMOUNT_COL))));
        // (DOLocationID, (1, passengers, fare_amount))

        JavaPairRDD<Long, Tuple3<Integer, Double, Double>> aggr = rdd.reduceByKey((x, y) -> new Tuple3<>(x._1() + y._1(), x._2() + y._2(), x._3() + y._3()));
        // (DOLocationID, (somma_corse, somma_passengers, somma_fare_amount))
        aggr.mapToPair(tup -> new Tuple2<>(tup._2._1(), new Tuple3(tup._1, tup._2._2() / tup._2._1(), tup._2._3() / tup._2._1())))
                // (somma_corse, (DOLocationID, media_passengers, media_fare_amount))
                .sortByKey(false);
                // ordinata dalla somma corse maggiore alla minore

    }

}
