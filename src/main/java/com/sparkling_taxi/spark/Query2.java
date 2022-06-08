package com.sparkling_taxi.spark;

import com.sparkling_taxi.bean.query2.CSVQuery2;
import com.sparkling_taxi.bean.query2.Query2Bean;
import com.sparkling_taxi.bean.query2.Query2Calc;
import com.sparkling_taxi.bean.query2.Query2Result;
import com.sparkling_taxi.utils.Performance;
import com.sparkling_taxi.utils.Utils;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.storage.StorageLevel;
import redis.clients.jedis.Jedis;
import scala.Tuple2;

import java.util.HashMap;
import java.util.List;

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
        JavaRDD<CSVQuery2> csvListResult = jc.parallelize(result)
                // sometimes spark produces two partitions (two output files) but the output has only 3 lines,
                // so we force it to use only 1 partition
                .repartition(1)
                .map(CSVQuery2::new)
                .cache();

        // Dataframe is NOT statically typed, but uses less memory (GC) than dataset
        spark.createDataFrame(csvListResult, CSVQuery2.class)
                .select("hour",
                        new String[]{"avgTip",
                                "stdDevTip",
                                "popPayment",
                                "locationDistribution"}) // to set the correct order of columns!
                .write()
                .mode("overwrite")
                .option("header", true)
                .option("delimiter", ";")
                .csv(OUT_DIR_Q1);

        System.out.println("================== written to HDFS =================");

        // result.forEach(System.out::println);

        // REDIS
        try (Jedis jedis = new Jedis("redis://redis:6379")) {
            for (CSVQuery2 t : csvListResult.collect()) {
                HashMap<String, String> m = new HashMap<>();
                m.put("Hour", t.getHour());
                m.put("Avg Tip", String.valueOf(t.getAvgTip()));
                m.put("Std Dev Tip", String.valueOf(t.getStdDevTip()));
                m.put("Most Popular Payment", String.valueOf(t.getStdDevTip()));
                for (Long i = 1L; i <= NUM_LOCATIONS; i++) {
                    m.put("Loc"+i, String.valueOf(t.getLocationDistribution().get(i)));
                }
                jedis.hset(t.getHour(), m);
            }
        }
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
