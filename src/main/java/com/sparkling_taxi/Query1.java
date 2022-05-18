package com.sparkling_taxi;


import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat;
import scala.Tuple2;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static sun.font.FontUtilities.isWindows;

public class Query1 {

    public static final String FILE_1 = "hdfs://namenode:9000/home/dataset-batch/yellow_tripdata_2021-12.parquet";
    public static final String FILE_2 = "hdfs://namenode:9000/home/dataset-batch/yellow_tripdata_2022-01.parquet";
    public static final String FILE_3 = "hdfs://namenode:9000/home/dataset-batch/yellow_tripdata_2022-02.parquet";
    public static final String FILE_Q1 = "hdfs://namenode:9000/home/dataset-batch/Query1.parquet";
    public static final String FILE_CIAO = "hdfs://namenode:9000/home/dataset-batch/ciao.txt";
    public static final String OUT_DIR = "hdfs://namenode:9000/home/dataset-batch/output-query1/";
    public static final int PASSENGER_COUNT_COL = 3;

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

    /**
     * A working example that prints the first row of a parquet file
     */
    private static void workingJob(){
        try (SparkSession spark = SparkSession
                .builder()
                .master("spark://spark:7077")
                .appName("Query1")
                .getOrCreate();
        ) {
            // spark.sparkContext().setLogLevel("WARN");
            System.out.println("======================= before batch =========================");
            JavaRDD<Row> batch1 = spark.read().format("parquet").load(FILE_1).toJavaRDD();
            System.out.println("======================= before map =========================");
            JavaRDD<String> stringArrRDD = batch1.map(row -> {
                StringBuilder sb = new StringBuilder();
                int length = row.length();
                for (int i = 0; i < length; i++) {
                    sb.append(row.get(i) != null ? row.get(i).toString() : "null").append("\t");
                }
                return sb.toString();
            });

            System.out.println(stringArrRDD.first());

            System.out.println("====<<<>>0>=>=>===>==== Done ======>=>><><>>0<00>>==>=>=");
        }
    }

    public static double passengerMean(JavaRDD<Double> passengers) {
        passengers.cache();
        // action that counts the number of elements in the java rdd
        long num = passengers.count();
        System.out.println("number of elements: " + num);
        // action that sums each value in the rdd
        double sum = passengers.reduce(Double::sum); // same thing as: (a, b) -> Double.sum(a, b)
        System.out.println("number of passengers: " + sum);
        return sum / (double) num;
    }

    public static void singleMonthMean(SparkSession spark, String file, List<Tuple2<String,Double>> means) {
        System.out.println("======================= before passengers =========================");
        JavaRDD<Double> passengers = spark.read().parquet(file)
                .toJavaRDD()
                .map(row -> {
                    try {
                        return row.getDouble(3);
                    } catch (NullPointerException e){
                        return 0.0;
                    }
                })
                .cache();
        
        System.out.println("======================= before mean =========================");
        Double sum = passengers.reduce(Double::sum);
        long num = passengers.count();
        System.out.println("number of elements: " + num);
        System.out.println("number of passengers: " + sum);
//        Double mean = passengers.reduce(Double::sum)/passengers.count();
        Double mean = sum / (double) num;
        System.out.println("Mean number of passengers 2021-12: " + mean);
        means.add(new Tuple2<>(Arrays.asList(file.split("/")).get(file.split("/").length-1), mean));
    }

    public static void monthMean(SparkSession spark, String file, List<Tuple2<String,Double>> means) {
        System.out.println("======================= before passengers =========================");
        JavaRDD<Double> passengers = spark.read().parquet(file)
                .toJavaRDD()
                .map(row -> {
                    try {
                        return row.getDouble(3);
                    } catch (NullPointerException e){
                        return 0.0;
                    }
                })
                .cache();
        System.out.println("======================= before mean =========================");
        Double sum = passengers.reduce(Double::sum);
        long num = passengers.count();
        System.out.println("number of elements: " + num);
        System.out.println("number of passengers: " + sum);
//        Double mean = passengers.reduce(Double::sum)/passengers.count();
        Double mean = sum / (double) num;
        System.out.println("Mean number of passengers 2021-12: " + mean);
        means.add(new Tuple2<>(Arrays.asList(file.split("/")).get(file.split("/").length-1), mean));
    }

    public static void main(String[] args) {
        // TODO: chiamare NiFi da qui

        // Must install WINUTILS.EXE for Windows.
        // windowsCheck();

        try (SparkSession spark = SparkSession
                .builder()
                .master("spark://spark:7077")
                .appName("Query1")
                .getOrCreate();
        ) {
            spark.sparkContext().setLogLevel("WARN");
            List<Tuple2<String,Double>> means = new ArrayList<>();
            singleMonthMean(spark, FILE_1, means);
            singleMonthMean(spark, FILE_2, means);
            singleMonthMean(spark, FILE_3, means);
            singleMonthMean(spark, FILE_Q1, means);
            JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());
            JavaRDD<Tuple2<String, Double>> result = sc.parallelize(means);
            result.collect().forEach(System.out::println);
            sc.close();
        }

    }
}
