package com.sparkling_taxi;


import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;
import scala.Tuple3;

import java.io.File;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;

// docker cp backup/Query1.parquet namenode:/home/Query1.parquet
// hdfs dfs -put Query1.parquet /home/dataset-batch/Query1.parquet
public class Query1 {


    public static final String FILE_Q1 = "hdfs://namenode:9000/home/dataset-batch/Query1.parquet";
    public static final String OUT_DIR = "hdfs://namenode:9000/home/dataset-batch/output-query1/";
    public static final String DIR_TIMESTAMP = "hdfs://namenode:9000/home/dataset-batch/timestamp";
    public static final int PASSENGER_COUNT_COL = 2;
    public static final int DROP_OUT_COL = 1;

    /**
     * @return true if on windows is installed C:\\Hadoop\\hadoop-2.8.1\\bin\\WINUTILS.EXE
     */
    private static boolean windowsCheck() {
        if (System.getProperty("os.name").equals("windows")) {
            System.out.println("Hi");
            if (new File("C:\\Hadoop\\hadoop-2.8.1").exists()) {
                System.setProperty("hadoop.home.dir", "C:\\Hadoop");
            } else {
                System.out.println("Install WINUTIL.EXE from and unpack it in C:\\Windows");
                return false;
            }
        }
        return true;
    }

    public static JavaPairRDD<Tuple2<Integer, Integer>, Double> multiMonthMeanV2(SparkSession spark, String file) {
        System.out.println("======================= before passengers =========================");
        JavaPairRDD<Timestamp, Double> passengersTS = spark.read().parquet(file)
                .toJavaRDD()
                .filter(row -> !(row.isNullAt(DROP_OUT_COL) || row.isNullAt(PASSENGER_COUNT_COL))) //TODO: questo andrebbe fatto su NiFi
                .mapToPair(row -> new Tuple2<>(row.getTimestamp(DROP_OUT_COL), row.getDouble(PASSENGER_COUNT_COL)));
        // (timestamp, double)

        System.out.println("instances: " + passengersTS.count());

        JavaPairRDD<Tuple2<Integer, Integer>, Double> tuple2 = passengersTS.mapToPair(tup -> {
            LocalDateTime ld = Utils.toLocalDateTime(tup._1);
            return new Tuple2<>(new Tuple2<>(ld.getMonthValue(), ld.getYear()), tup._2);
        }); //.persist(StorageLevel.MEMORY_AND_DISK_SER()); // ((month, year), passengers)

        JavaPairRDD<Tuple2<Integer, Integer>, Tuple2<Double, Integer>> aggTuple = tuple2
                .mapToPair(tup -> new Tuple2<>(tup._1, new Tuple2<>(tup._2, 1))) // ((month, year), (passengers, 1))
                .reduceByKey((x, y) -> new Tuple2<>(x._1 + y._1, x._2 + y._2)); // ((month, year), (sum_passengers, sum_instances))

        aggTuple.collect().forEach(System.out::println);

        return aggTuple.mapToPair(tup -> new Tuple2<>(tup._1, tup._2._1 / (double) tup._2._2));
    }

    public static void main(String[] args) {
        // TODO: chiamare NiFi da qui

        // Must install WINUTILS.EXE for Windows.
        // windowsCheck();

        SparkSession spark = SparkSession
                .builder()
                .master("spark://spark:7077")
                .appName("Query1")
                .getOrCreate();
        spark.sparkContext().setLogLevel("WARN");

        JavaPairRDD<Tuple2<Integer, Integer>, Double> javaPairRDD = Performance.measure("Query completa", () -> multiMonthMeanV2(spark, FILE_Q1));
        javaPairRDD.collect().forEach(x -> System.out.println("(Month,Year): " + x._1 + ", mean passengers: " + x._2));
//  JavaSparkContext sc = JavaSparkContext.fromSparkContext(spark.sparkContext());
//            JavaRDD<Tuple2<String, Double>> result = sc.parallelize(means);
//            result.collect().forEach(System.out::println);
        spark.close();
    }
}