package com.sparkling_taxi.sparksql;

import com.sparkling_taxi.bean.query2.CSVQuery2;
import com.sparkling_taxi.bean.query2.Query2Result;
import com.sparkling_taxi.evaluation.Performance;
import com.sparkling_taxi.spark.Query;
import com.sparkling_taxi.spark.Query2;
import com.sparkling_taxi.utils.Utils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.ArrayList;
import java.util.List;

import static com.sparkling_taxi.utils.Const.FILE_Q2;
import static com.sparkling_taxi.utils.Const.PRE_PROCESSING_TEMPLATE_Q2;
import static org.apache.spark.sql.functions.*;

public class QuerySQL2 extends Query<CSVQuery2> {

    public static void main(String[] args) {
        QuerySQL2 sql2 = new QuerySQL2();
        try {
            sql2.preProcessing();
            List<CSVQuery2> csvList = Performance.measure(sql2::processing);
            sql2.postProcessing(csvList);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void preProcessing() {
        Utils.doPreProcessing(FILE_Q2, PRE_PROCESSING_TEMPLATE_Q2);
    }

    /**
     * Hourly distribution of
     * - percentage of trips each hour each start location
     * - standard deviation of tips each hour
     * - most popular payment method each hour (MAX number of occurrence)
     */
    @Override
    public List<CSVQuery2> processing() {
        System.out.println("======================= Running " + this.getClass().getSimpleName() + " =======================");

        spark.read().parquet(FILE_Q2)
                .toDF("pickup", "pu_location", "tip_amount", "payment_type")
                .withColumn("hour", date_trunc("hour", col("pickup"))) // gets the date and the hour
                .createOrReplaceTempView("query2");

        Dataset<Row> hourlyTips = spark.sql("SELECT hour, avg(tip_amount) as avg_tip, stddev(tip_amount) as stddev_tip " +
                                            "FROM query2 " +
                                            "GROUP BY hour ");
        hourlyTips.createOrReplaceTempView("hourly_tips");

        Dataset<Row> locDistrib = spark.sql("SELECT hour, pu_location, count(pu_location) as loc_count " +
                                            "FROM query2 " +
                                            "GROUP BY hour, pu_location ");
        locDistrib.createOrReplaceTempView("loc_distribution");

        Dataset<Row> locCountHourly = spark.sql("SELECT hour, count(pu_location) as hour_loc_count " +
                                                "FROM query2 " +
                                                "GROUP BY hour");
        locDistrib.createOrReplaceTempView("hourly_loc_count");

        Dataset<Row> paymentDistrib = spark.sql("SELECT hour, payment_type, count(*) as occurrences " +
                                                "FROM query2 " +
                                                "GROUP BY hour, payment_type " +
                                                "ORDER BY hour");
        paymentDistrib.createOrReplaceTempView("payment_distribution");

        List<Row> rows = spark.sql("SELECT hour, payment_type, occurrences, row_number() over (PARTITION BY hour ORDER BY occurrences DESC) as rank " +
                                   "FROM payment_distribution ")
                .where("rank = 1")
                .select("hour", "payment_type")
                .join(hourlyTips, "hour")
                .join(locDistrib, "hour")
                .join(locCountHourly, "hour")
                .selectExpr("hour", "avg_tip", "stddev_tip", "query2.pu_location", "loc_count / hour_loc_count as loc_distrib", "payment_type")
                .collectAsList();

        /*
         * +-------------------+------------------+------------------+-----------+--------------------+------------+
         * |               hour|           avg_tip|        stddev_tip|pu_location|         loc_distrib|payment_type|
         * +-------------------+------------------+------------------+-----------+--------------------+------------+
         * |2021-12-01 00:00:00|2.9100474683544335|3.7732257355704832|        141|  0.0189873417721519|           1|
         */

//        List<Query2Result> query2Results = new ArrayList<>();
//        List<Double> locDistrDoubles = new ArrayList<>();
//        for (Row row : rows) {
//            query2Results.add(new Query2Result(row.getTimestamp(0).toString(),
//                    row.getDouble(1),
//                    row.getDouble(2),
//                    row.getLong(3),
//                    "todo"));
//        }

        List<CSVQuery2> csvQuery2 = new ArrayList<>();


        return csvQuery2;
    }

    @Override
    public void postProcessing(List<CSVQuery2> result) {
        Query2.storeToCSVOnHDFS(result, this);
        System.out.println("================== written csv to HDFS =================");

        Query2.storeQuery2ToRedis(result);
        System.out.println("================== copied csv to local FS =================");
    }
}
