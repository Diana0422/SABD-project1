package com.sparkling_taxi;


import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.List;

public class Query1 {

    public static final String FILE_1 = "hdfs://namenode:9000/home/dataset-batch/yellow_tripdata_2021-12.parquet";
    public static final String FILE_2 = "hdfs://namenode:9000/home/dataset-batch/ciao.txt";
    public static final String OUT_DIR = "hdfs://namenode:9000/home/dataset-batch/output-query1/";
    public static final int PASSENGER_COUNT_COL = 3;


    public static double passengerMean(JavaRDD<Integer> passengers) {
        // actions that counts the number of elements in the java rdd
        long num = passengers.count();
        System.out.println(num);
        // action that sums each value in the rdd
        long sum = passengers.reduce(Integer::sum);
        System.out.println(sum);
        return (double) sum / (double) num;
    }

    public static void main(String[] args) {
        // TODO: chiamare NiFi da qui

        try (SparkSession spark = SparkSession
                .builder()
                .appName("JavaWordCount")
                .getOrCreate();
        ) {
            System.out.println("======================= before batch =========================");
            JavaRDD<Row> batch1 = spark.read().format("parquet").load(FILE_1).toJavaRDD().cache();

            batch1.collect().stream().map(row -> row.get(3)).forEach(System.out::println);
            System.out.println("======================= before passengers =========================");
            JavaRDD<Integer> passengers = batch1.map(row -> row.getInt(3));

//            System.out.println(passengers.collect());
//            System.out.println("======================= before mean =========================");
//            double mean = passengerMean(passengers);
//            System.out.println("Mean number of passengers 2021-12: " + mean);

            batch1.unpersist();
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
