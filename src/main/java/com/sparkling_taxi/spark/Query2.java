package com.sparkling_taxi.spark;

import com.sparkling_taxi.bean.query2.*;
import com.sparkling_taxi.utils.Performance;
import com.sparkling_taxi.utils.Utils;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;

import java.util.*;

import static com.sparkling_taxi.utils.Const.*;

// docker cp backup/Query2.parquet namenode:/home/Query2.parquet
// hdfs dfs -put Query2.parquet /home/dataset-batch/Query2.parquet
public class Query2 extends Query<Query2Result> {

    public static void main(String[] args) {
        Query2 q = new Query2();
        q.preProcessing();
        List<Query2Result> result = q.processing();
        q.postProcessing(result);
        q.closeSession();
    }

    public Query2() {
        super();
    }

    public void preProcessing() {
        Utils.doPreProcessing(FILE_Q2, PRE_PROCESSING_TEMPLATE_Q2);
    }

    public List<Query2Result> processing() {
        return Performance.measure("Query completa", () -> query2PerHourWithGroupBy(spark, FILE_Q2));
    }

    public void postProcessing(List<Query2Result> result) {
        //TODO grafana redis interaction
    }

    /**
     * Hourly distribution of
     * - mean number of trips each hour
     * - mean of tips each hour
     * - standard deviation of tips each hour
     * - most popular payment method each hour (MAX number of occurrence)
     */
    public static List<Query2Result> query2PerHourWithGroupBy(SparkSession spark, String file) {
        JavaRDD<Query2Bean> rdd = spark.read().parquet(file)
                .as(Encoders.bean(Query2Bean.class))
                .toJavaRDD();
        // (hour, Query2Calc(trip_count=1, tip_amount, square_tip_amount, payment_type_distribution, PULocation_trips_distribution))
        // Payment_type_distribution is a Map with value 1 for the payment type of the trip and 0 for the other 5 payment types
        // PULocation_trips_distribution is a Map with value 1 for the departure location of the trip and 0 for the other 264 locations
        JavaPairRDD<String, Query2Calc> hourCalc = rdd.mapToPair(bean -> new Tuple2<>(Utils.getHourDay(bean.getTpep_pickup_datetime()), new Query2Calc(1, bean)));
        // sums all previous data and the result is:
        // (hour, Query2Calc(total_trip_count, sum_of_tips, sum_of_square_tips, final_payment_type_distribution, final_PULocation_trips_distribution))
        // where final_payment_type_distribution is a Map with the total number of trips paid with each payment type
        // and final_PULocation_trips_distribution is a Map with the total number of trips from each location
        JavaPairRDD<String, Query2Calc> reduced = hourCalc.reduceByKey(Query2Calc::sumWith);
        // computes means, standard deviations, max payment types and location distribution of trips for each hour
        // (hour, Query2Result(avgTip, stdDevTip, mostPopularPaymentType, distribution_of_trips_for_265_locations)
        // where distribution_of_trips_for_265_locations is a Map with the percentage of trips from each location
        JavaRDD<Query2Result> result = reduced.map(Query2Result::new);
        return result.persist(StorageLevel.MEMORY_ONLY_SER()).collect();
    }
}
