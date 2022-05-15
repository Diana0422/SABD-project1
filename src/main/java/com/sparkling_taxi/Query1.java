package com.sparkling_taxi;


import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.List;

public class Query1 {

    public static final String FILE_1 = "hdfs://namenode:9000/home/dataset-batch/yellow_tripdata_2021-12.parquet";
    public static final String FILE_2 = "hdfs://namenode:9000/home/dataset-batch/ciao.txt";


    public static void main(String[] args) throws Exception {
        // TODO: chiamare NiFi da qui


        SparkConf conf = new SparkConf()
                .setMaster("local")
                .setAppName("Query1");
        JavaSparkContext sc = new JavaSparkContext(conf);

        System.out.println("====<<<>>0>=>=>===>==== Spark context ok ======>=>><><>>0<00>>==>=>=");
        // List<String> rows = spark.read().textFile(FILE_2).javaRDD().collect();
        List<String> collect = sc.textFile(FILE_2).collect();
        collect.stream().limit(10).forEach(System.out::println);
//        JavaRDD<String> ds = spark.read().parquet(FILE_1).javaRDD().collect();
//
//        System.out.println("====<<<>>0>=>=>===>==== Read ok ======>=>><><>>0<00>>==>=>=");
//        rows.stream()
//                .limit(10)
//                .forEach(System.out::println);

        sc.stop();
    }
}
