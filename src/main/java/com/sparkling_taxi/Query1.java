package com.sparkling_taxi;


import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat;

import java.io.File;
import java.util.List;

import static sun.font.FontUtilities.isWindows;

public class Query1 {

    public static final String FILE_1 = "hdfs://namenode:9000/home/dataset-batch/yellow_tripdata_2021-12.parquet";
    public static final String FILE_2 = "hdfs://namenode:9000/home/dataset-batch/ciao.txt";
    public static final String OUT_DIR = "hdfs://namenode:9000/home/dataset-batch/output-query1/";
    public static final int PASSENGER_COUNT_COL = 3;


    public static double passengerMean(JavaRDD<Double> passengers) {
        // actions that counts the number of elements in the java rdd
        long num = passengers.count();
        System.out.println(num);
        // action that sums each value in the rdd
        double sum = passengers.reduce(Double::sum);
        System.out.println(sum);
        return (double) sum / (double) num;
    }

    public static void main(String[] args) {
        // TODO: chiamare NiFi da qui

        // Must install WINUTILS.EXE for Windows :
        // check if system is Windows

//        if (System.getProperty("os.name").equals("windows")) {
//            System.out.println("Hi");
//            if (new File("C:\\Hadoop\\hadoop-2.8.1").exists()) {
//                System.setProperty("hadoop.home.dir", "C:\\Hadoop");
//            } else {
//                System.out.println("Install WINUTIL.EXE from and unpack it in C:\\Windows");
//                return;
//            }
//        }
//        try (SparkSession spark = SparkSession
//                .builder()
//                .master("local")
//                .appName("Query1")
//                .getOrCreate();
//             JavaSparkContext jSpark = new JavaSparkContext(spark.sparkContext())
//        ) {
//            System.out.println("======================= before batch =========================");
//            // jSpark.hadoopFile(FILE_1, ParquetFileFormat.class, Integer.class, Object.class);
//
//            JavaRDD<Row> batch1 = spark.read().parquet(FILE_1).toJavaRDD();
//
//            batch1.collect().stream().map(row -> row.get(3)).forEach(System.out::println);
//            System.out.println("======================= before passengers =========================");
//            JavaRDD<Double> passengers = batch1.map(row -> row.getDouble(3));
//
//            System.out.println(passengers.collect());
//            System.out.println("======================= before mean =========================");
//            double mean = passengerMean(passengers);
//            System.out.println("Mean number of passengers 2021-12: " + mean);
//
//            batch1.unpersist();
//        }
//
//        //map + reducebykey
//
//        // passengers.map()
//
//        // .map(row -> row.get(1).toString())
//        // .coalesce(1)
//        // .saveAsTextFile(OUT_DIR);
//
//        // spark.read().parquet(FILE_1).write().mode("overwrite").parquet(OUT_DIR);
//
//        // spark.sparkContext().textFile(FILE_OUT, 1);
//
//        System.out.println("====<<<>>0>=>=>===>==== Write done ======>=>><><>>0<00>>==>=>=");
//
////        List<String> strings = spark.read().textFile(OUT_DIR).limit(10).collectAsList();
////        strings.forEach(System.out::println);
//
//
////        JavaRDD<String> lines = spark.read().textFile(args[0]).javaRDD();
//
////        JavaRDD<String> words = lines.flatMap(s -> Arrays.asList(SPACE.split(s)).iterator());
////
////        JavaPairRDD<String, Integer> ones = words.mapToPair(s -> new Tuple2<>(s, 1));
////
////        JavaPairRDD<String, Integer> counts = ones.reduceByKey((i1, i2) -> i1 + i2);
////
////        List<Tuple2<String, Integer>> output = counts.collect();
////        for (Tuple2<?,?> tuple : output) {
////            System.out.println(tuple._1() + ": " + tuple._2());
////        }
//
//        /*SparkConf conf = new SparkConf()
//                .setMaster("local")
//                .setAppName("Query1");
//
//        // try-with-resources (closes automatically the spark context)
//        try (JavaSparkContext sc = new JavaSparkContext()){
//            System.out.println("====<<<>>0>=>=>===>==== Spark context ok ======>=>><><>>0<00>>==>=>=");
//            // List<String> rows = spark.read().textFile(FILE_2).javaRDD().collect();
//
//            // spark query
//            List<String> collect = sc.textFile(FILE_2).collect();
//
//            // java 8 stream API to print only 10 rows
//            collect.stream().limit(10).forEach(System.out::println);
//
//            // TODO: bisogna lavorare direttamente sui parquet (o in alternativa lavorare sul CSV)
//            // JavaRDD<String> ds = spark.read().parquet(FILE_1).javaRDD().collect();
//        }
//
//         */
        try (SparkSession spark = SparkSession
                .builder()
                .master("spark://spark:7077")
                .appName("Query1")
                .getOrCreate();
        ) {
            // spark.sparkContext().setLogLevel("WARN");
            System.out.println("======================= before batch =========================");
            JavaRDD<Row> batch1 = spark.read().format("parquet").load(FILE_1)
                    .toJavaRDD();
            System.out.println("======================= before passengers =========================");
            JavaRDD<String> stringArrRDD = batch1.map(row -> {
                StringBuilder sb = new StringBuilder();
                int length = row.length();
                for (int i = 0; i < length; i++) {
                    sb.append(row.get(i) != null ? row.get(i).toString() : "");
                }
                return sb.toString();
            });

            stringArrRDD.collect().forEach(System.out::println);


//            System.out.println(passengers.collect());
//            System.out.println("======================= before mean =========================");
//            double mean = passengerMean(passengers);
//            System.out.println("Mean number of passengers 2021-12: " + mean);

            // batch1.unpersist();
        }

        //map + reducebykey


//        passengers.map()


//                .map(row -> row.get(1).toString())
//                .coalesce(1)
//                .saveAsTextFile(OUT_DIR);

        // spark.read().parquet(FILE_1).write().mode("overwrite").parquet(OUT_DIR);

        // spark.sparkContext().textFile(FILE_OUT, 1);

        System.out.println("====<<<>>0>=>=>===>==== Write done ======>=>><><>>0<00>>==>=>=");

//        List<String> strings = spark.read().textFile(OUT_DIR).limit(10).collectAsList();
//        strings.forEach(System.out::println);


//        JavaRDD<String> lines = spark.read().textFile(args[0]).javaRDD();

//        JavaRDD<String> words = lines.flatMap(s -> Arrays.asList(SPACE.split(s)).iterator());
//
//        JavaPairRDD<String, Integer> ones = words.mapToPair(s -> new Tuple2<>(s, 1));
//
//        JavaPairRDD<String, Integer> counts = ones.reduceByKey((i1, i2) -> i1 + i2);
//
//        List<Tuple2<String, Integer>> output = counts.collect();
//        for (Tuple2<?,?> tuple : output) {
//            System.out.println(tuple._1() + ": " + tuple._2());
//        }

        /*SparkConf conf = new SparkConf()
                .setMaster("local")
                .setAppName("Query1");

        // try-with-resources (closes automatically the spark context)
        try (JavaSparkContext sc = new JavaSparkContext()){
            System.out.println("====<<<>>0>=>=>===>==== Spark context ok ======>=>><><>>0<00>>==>=>=");
            // List<String> rows = spark.read().textFile(FILE_2).javaRDD().collect();

            // spark query
            List<String> collect = sc.textFile(FILE_2).collect();

            // java 8 stream API to print only 10 rows
            collect.stream().limit(10).forEach(System.out::println);

            // TODO: bisogna lavorare direttamente sui parquet (o in alternativa lavorare sul CSV)
            // JavaRDD<String> ds = spark.read().parquet(FILE_1).javaRDD().collect();
        }

         */
    }
}
