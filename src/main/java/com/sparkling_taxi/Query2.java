package com.sparkling_taxi;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;
import scala.Tuple4;
import scala.Tuple5;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

// docker cp backup/Query2.parquet namenode:/home/Query2.parquet
// hdfs dfs -put Query2.parquet /home/dataset-batch/Query2.parquet
public class Query2 {

    public static final String FILE_Q2 = "hdfs://namenode:9000/home/dataset-batch/Query2.parquet";
    public static final int PICKUP_COL = 0;
    public static final int DROPOFF_COL = 1;
    public static final int TIP_AMOUNT_COL = 2;
    public static final int PAYMENT_TYPE_COL = 3;

    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .master("spark://spark:7077")
                .appName("Query1")
                .getOrCreate();
        spark.sparkContext().setLogLevel("WARN");

        JavaPairRDD javaPairRDD = Performance.measure("Query completa", () -> query2PerHour(spark, FILE_Q2));
    }

    /**
     * Hourly distribution of
     * - mean number od trips each hour
     * - mean AND standard deviation of tips each hour
     * - most popular payment method each hour (MAX number of occurrence)
     * TODO: dovremmo raggruppare per ora, quindi avere 24 chiavi diverse.
     *
     * @param spark
     * @param file
     */
    private static JavaPairRDD query2PerHour(SparkSession spark, String file) {
        spark.read().parquet(file)
                .toJavaRDD()
                .filter(row -> {
                    boolean x = true;
                    for (int i = 0; i < row.size(); i++) {
                        x = x && !row.isNullAt(i);
                    }
                    return x;
                }).flatMap(row -> {
                    Timestamp timestamp = row.getTimestamp(PICKUP_COL);
                    Timestamp timestamp2 = row.getTimestamp(DROPOFF_COL);
                    int hourStart = Utils.toLocalDateTime(timestamp).getHour();
                    int hourEnd = Utils.toLocalDateTime(timestamp2).getHour();

                    List<Tuple5<Integer, Integer, Double, Double, Integer>> tups = new ArrayList<>();
                    List<Integer> integers = hourSlots(hourStart, hourEnd);
                    for (Integer hour : integers) {
                        Tuple5<Integer, Integer, Double, Double, Integer> tup5 = new Tuple5<>(
                                hour,
                                1, // counts the trips each hour
                                row.getDouble(TIP_AMOUNT_COL),
                                row.getDouble(TIP_AMOUNT_COL) * row.getDouble(TIP_AMOUNT_COL),
                                row.getInt(PAYMENT_TYPE_COL));
                        tups.add(tup5);
                    }

                    return tups.iterator();
                    // tuple2 : (hour, (1, tip_amount, square_tip_amount, payment_tipe))
                }).mapToPair(tuple5 -> new Tuple2<>(tuple5._1(), new Tuple4<>(tuple5._2(), tuple5._3(), tuple5._4(), tuple5._5())))
                // tuple2: (hour, (hour_taxi_trips, hour_tip_amount, hour_square_tip_amount, FIXME: sum_payment_type))
                .reduceByKey((x, y) -> new Tuple4<>(x._1() + y._1(), x._2() + y._2(), x._3() + y._3(), x._4() + y._4())) //TODO: attenzione molto sbagliato l'ultimo elemento

                .mapToPair(x -> new Tuple2<>(x._1(), new Tuple4<>(
                        x._2._1(), // hour_trips
                        x._2._2() / x._2._1(), // mean_hour_tip_amount
                        Utils.stddev(x._2._1(), x._2()._2(), x._2()._3()), // stddev_hour_tip_amount
                        Utils.argMax(x._2._4(), Arrays.asList(2,3,5)))));
        return null;
    }

    public static List<Integer> hourSlots(int hourStart, int hourEnd) {
        // hour zone are: 0,1,2,3,...,23 where 0 = 00:00-00:59, 23=23:00-23:59
        if (hourStart == hourEnd) {
            return Collections.singletonList(hourStart);
        }
        List<Integer> list = new ArrayList<>();
        int counter = hourStart;
        while (counter != hourEnd + 1) {
            list.add(counter);
            counter = (counter + 1) % 24;
        }

        return list;
    }
}
