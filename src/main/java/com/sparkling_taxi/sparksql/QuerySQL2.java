package com.sparkling_taxi.sparksql;

import com.sparkling_taxi.bean.query2.CSVQuery2;
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

        Dataset<Row> paymentDistrib = spark.sql("SELECT hour, payment_type, count(*) as occurrences " +
                                                "FROM query2 " +
                                                "GROUP BY hour, payment_type " +
                                                "ORDER BY hour");
        paymentDistrib.createOrReplaceTempView("payment_distribution");

        spark.sql("SELECT hour, payment_type, occurrences, row_number() over (PARTITION BY hour ORDER BY occurrences DESC) as rank " +
                  "FROM payment_distribution ")
                .where("rank = 1")
                .select("hour", "payment_type")
                .join(hourlyTips, "hour")
                .join(locDistrib, "hour")
                .show(100);

        return new ArrayList<>();
    }

    @Override
    public void postProcessing(List<CSVQuery2> result) {
        Query2.storeToCSVOnHDFS(result, this);
        System.out.println("================== written csv to HDFS =================");

        Query2.storeQuery2ToRedis(result);
        System.out.println("================== copied csv to local FS =================");
    }
}
