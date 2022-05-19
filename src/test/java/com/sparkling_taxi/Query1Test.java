package com.sparkling_taxi;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

// import static org.junit.jupiter.api.Assertions.*;

class Query1Test {
    @Test
    public void passengerMean(){
        SparkSession spark = SparkSession
                .builder()
                .master("local")
                .appName("Query1")
                .getOrCreate();
        JavaSparkContext j = new JavaSparkContext(spark.sparkContext());
        List<Double> list = Arrays.asList(2.0,1.0,3.0,2.0,1.0,2.0);
        JavaRDD<Double> rdd = j.parallelize(list);
        // double mean = Query1.passengerMean(rdd);

        // assertEquals((double) (2+1+3+2+1+2)/6, mean);
        j.close();
        spark.close();
    }
}