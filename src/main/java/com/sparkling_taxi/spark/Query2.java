package com.sparkling_taxi.spark;

import com.sparkling_taxi.bean.query2.*;
import com.sparkling_taxi.bean.query3.Query3Calc;
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
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

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

    public static JavaPairRDD query2V2(SparkSession spark, String file) {
        JavaRDD<Query2Bean> rdd = spark.read().parquet(file)
                .as(Encoders.bean(Query2Bean.class))
                .toJavaRDD()
                .cache();
        // partial 1: calculate total trips + total tip + distribution
        JavaPairRDD<String, Query2CalcV2> hourCalc = rdd.mapToPair(bean -> new Tuple2<>(Utils.getHourDay(bean.getTpep_pickup_datetime()), new Query2CalcV2(1, bean)));
        JavaPairRDD<String, Query2CalcV2> reduced1 = hourCalc.reduceByKey(Query2CalcV2::sumWith);
        JavaPairRDD<String, Query2ResultV2> partial1 = reduced1.mapValues(Query2ResultV2::new).cache();


        // partial 2: calculate number of trips for each PULocation
        JavaPairRDD<Tuple2<String, Long>, Integer> locationMapping = rdd.mapToPair(bean -> new Tuple2<>(new Tuple2<>(Utils.getHourDay(bean.getTpep_pickup_datetime()), bean.getPayment_type()), 1));
        JavaPairRDD<Tuple2<String, Long>, Integer> reduced = locationMapping.reduceByKey(Integer::sum); // ((dayHour, paymentType), 1) -> ((dayHour, paymentType), occorrenzaxtipo))

        JavaPairRDD<String, Tuple2<Long, Integer>> result = reduced.mapToPair(t -> new Tuple2<>(t._1._1, new Tuple2<>(t._1._2, t._2))) // ((dayHour, paymentType), occorrenzaxtipo)) -> (dayHour, (paymentType, occorrenza))
                .reduceByKey((t1, t2) -> { // (dayHour, (paymentType1, occorrenza)) , (dayHour, (paymentType2, occorrenza))
                    if (t1._2 >= t2._2) return t1;
                    else return t2;
                }).cache(); // (dayHour, (payment, occurrence))

        JavaPairRDD<String, Tuple2<Tuple2<Long, Integer>, Query2ResultV2>> join = result.join(partial1).cache();
        join.collect();
        return null;
    }

    private enum PaymentType {
        CreditCard(1),
        Cash(2),
        NoCharge(3),
        Dispute(4),
        Unknown(5),
        VoidedTrip(6);
        private final int num;

        PaymentType(int num) {
            this.num = num;
        }

        public int getNum() {
            return num;
        }
    }
}
