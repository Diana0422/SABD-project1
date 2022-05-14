package com.sparkling_taxi;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.List;

public class Query1 {

    public static final String FILE_1 = "hdfs://namenode:9000/home/dataset-batch/yellow_tripdata_2021-12.parquet";

    public static void main(String[] args) {
//        if (args.length < 1) {
//            System.err.println("Usage: JavaWordCount <file>");
//            System.exit(1);
//        }

        SparkSession spark = SparkSession
                .builder()
                .master("local")
                .appName("Query1")
                .config("HADOOP_HOME", "hdfs://namenode:9000/")
                .config("hadoop.home.dir", "hdfs://namenode:9000/")
                .getOrCreate();

        var collect = spark.read().parquet(FILE_1).collectAsList();
        for (Row row : collect) {
            System.out.println(row);
        }

//        JavaRDD<String> words = lines.flatMap(s -> Arrays.asList(SPACE.split(s)).iterator());
//
//        JavaPairRDD<String, Integer> ones = words.mapToPair(s -> new Tuple2<>(s, 1));
//
//        JavaPairRDD<String, Integer> counts = ones.reduceByKey((i1, i2) -> i1 + i2);
//
//        List<Tuple2<String, Integer>> output = counts.collect();
//        for (JavaRDD<String> tuple : collect.toLocalIterator()) {
//            System.out.println(tuple);
//        }
        spark.stop();
    }
}
