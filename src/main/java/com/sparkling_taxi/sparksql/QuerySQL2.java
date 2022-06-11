package com.sparkling_taxi.sparksql;

import com.sparkling_taxi.bean.query2.CSVQuery2;
import com.sparkling_taxi.evaluation.Performance;
import com.sparkling_taxi.spark.Query;
import com.sparkling_taxi.spark.Query2;
import com.sparkling_taxi.utils.Utils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;

import static com.sparkling_taxi.utils.Const.*;
import static org.apache.spark.sql.functions.*;

public class QuerySQL2 extends Query<CSVQuery2> {

    public QuerySQL2(SparkSession s) {
        super(s);
    }

    public QuerySQL2() {
        super();
    }

    public QuerySQL2(boolean b, SparkSession s) {
        super(b, s);
    }

    public static void main(String[] args) {
        QuerySQL2 sql2 = new QuerySQL2();
        try {
            sql2.preProcessing();
            List<CSVQuery2> csvList = Performance.measure(sql2::processing);
            sql2.postProcessing(csvList);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            sql2.closeSession();
        }
    }

    @Override
    public void preProcessing() {
        Utils.doPreProcessing(FILE_Q2, PRE_PROCESSING_TEMPLATE_Q2, forcePreprocessing);
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

        spark.sql("SELECT hour, payment_type, occurrences, row_number() over (PARTITION BY hour ORDER BY occurrences DESC) as rank " +
                  "FROM payment_distribution ")
                .where("rank = 1")
                .select("hour", "payment_type")
                .join(hourlyTips, "hour")
                .join(locDistrib, "hour")
                .join(locCountHourly, "hour")
                .createOrReplaceTempView("partial_result");

        List<Row> rows = spark.sql("SELECT hour, avg_tip, stddev_tip, concat_ws(';', partial_result.pu_location, loc_count / hour_loc_count) as loc_distr, payment_type " +
                                   "FROM partial_result " +
                                   "ORDER BY hour, loc_distr")
                .groupBy("hour", "avg_tip", "stddev_tip", "payment_type")
                .agg(collect_list("loc_distr").alias("all_loc_distr"))
                .collectAsList();
        List<CSVQuery2> query2 = new ArrayList<>();
        DecimalFormat decimalFormat = new DecimalFormat("#.######");
        for (Row r : rows) {
            String hour = r.getTimestamp(0).toString();
            Double avgTip = r.getDouble(1);
            Double stdDevTip = r.getDouble(2);
            Long payment = r.getLong(3);
            List<String> list = r.getList(4);
            Double[] array = new Double[NUM_LOCATIONS.intValue()];
            for (int i = 0; i < NUM_LOCATIONS; i++) {
                array[i] = 0.0;
            }
            for (String s : list) {
                String[] split = s.split(";");
                String loc = split[0];
                array[Integer.parseInt(loc) - 1] = Double.valueOf(split[1]);
            }
            StringBuilder s = new StringBuilder();

            for (int i = 0, arrayLength = array.length; i < arrayLength; i++) {
                Double d = array[i];
                s.append(decimalFormat.format(d));
                if (i != arrayLength - 1) {
                    s.append("-");
                }
            }
            query2.add(new CSVQuery2(hour, avgTip, stdDevTip, payment, s.toString()));
        }
        return query2;
    }

    @Override
    public void postProcessing(List<CSVQuery2> result) {
        Query2.storeToCSVOnHDFS(result, this);
        System.out.println("================== written csv to HDFS =================");

        Query2.storeQuery2ToRedis(result);
        System.out.println("================== copied csv to local FS =================");
    }
}
