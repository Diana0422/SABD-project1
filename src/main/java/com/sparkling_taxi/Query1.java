package com.sparkling_taxi;


import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.List;

public class Query1 {

    public static final String FILE_1 = "hdfs://namenode:9000/home/dataset-batch/yellow_tripdata_2021-12.parquet";
    public static final String FILE_2 = "hdfs://namenode:9000/home/dataset-batch/ciao.txt";


    public static void main(String[] args) {
        // TODO: chiamare NiFi da qui


        SparkConf conf = new SparkConf()
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
    }
}
