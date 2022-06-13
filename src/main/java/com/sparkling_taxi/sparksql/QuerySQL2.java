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

    public QuerySQL2(boolean forcePreprocessing, SparkSession s) {
        super(forcePreprocessing, s);
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

    /**
     * Does the preprocessing with using a fixed NiFi template, if the input file for processing is not already present.
     * If the field forcePreprocessing is true, the preprocessing is always done.
     * It also downloads the dataset, but only if they are not already downloaded on HDFS.
     */
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

        // reads the unified parquet from hdfs and truncates the date until the hour (like yyyy-MM-dd:hh)
        spark.read().parquet(FILE_Q2)
                .toDF("pickup", "pu_location", "tip_amount", "payment_type")
                .withColumn("hour", date_trunc("hour", col("pickup"))) // gets the date and the hour
                .createOrReplaceTempView("query2");

        // gets the average tipAmount, and its standard deviation
        Dataset<Row> hourlyTips = spark.sql("SELECT hour, avg(tip_amount) as avg_tip, \n" +
                                            "       stddev(tip_amount) as stddev_tip \n" +
                                            "FROM query2 \n" +
                                            "GROUP BY hour \n");
        hourlyTips.createOrReplaceTempView("hourly_tips");

        // gets the departure location count grouped by hour and location
        Dataset<Row> locDistrib = spark.sql("SELECT hour, pu_location, count(pu_location) as loc_count \n" +
                                            "FROM query2 \n" +
                                            "GROUP BY hour, pu_location\n");
        locDistrib.createOrReplaceTempView("loc_distribution");

        // gets the departure location count grouped only by hour
        Dataset<Row> locCountHourly = spark.sql("SELECT hour, count(pu_location) as hour_loc_count \n" +
                                                "FROM query2 \n" +
                                                "GROUP BY hour\n");
        locDistrib.createOrReplaceTempView("hourly_loc_count");

        // gets the count of trips for each hour and payment type.
        Dataset<Row> paymentDistrib = spark.sql("SELECT hour, payment_type, count(*) as occurrences \n" +
                                                "FROM query2 \n" +
                                                "GROUP BY hour, payment_type \n" +
                                                "ORDER BY hour\n");
        paymentDistrib.createOrReplaceTempView("payment_distribution");

        // gets the max payment type each hour and joins all the previous queries
        spark.sql("SELECT hour, payment_type, occurrences, \n" +
                  "       row_number() over (PARTITION BY hour ORDER BY occurrences DESC) as rank \n" +
                  "FROM payment_distribution \n")
                .where("rank = 1")
                .select("hour", "payment_type")
                .join(hourlyTips, "hour")
                .join(locDistrib, "hour")
                .join(locCountHourly, "hour")
                .createOrReplaceTempView("partial_result");

        // groups by each location for each hour in a single CELL e.g. (123;0.21,141;0.01,...)
        List<Row> rows = spark.sql("SELECT hour, avg_tip, stddev_tip, payment_type,\n" +
                                   "       concat_ws(';', partial_result.pu_location, loc_count / hour_loc_count) as loc_distr\n" +
                                   "FROM partial_result \n" +
                                   "ORDER BY hour, loc_distr\n")
                .groupBy("hour", "avg_tip", "stddev_tip", "payment_type")
                .agg(collect_list("loc_distr").alias("all_loc_distr"))
                .collectAsList();

        // all this converts from Row to CSVQuery2
        List<CSVQuery2> query2 = new ArrayList<>();
        DecimalFormat decimalFormat = new DecimalFormat("#.######");
        for (Row r : rows) {
            String hour = r.getTimestamp(0).toString();
            Double avgTip = r.getDouble(1);
            Double stdDevTip = r.getDouble(2);
            Long payment = r.getLong(3);
            List<String> list = r.getList(4); // collect_list produces a List<String>
            Double[] array = new Double[NUM_LOCATIONS.intValue()];
            for (int i = 0; i < NUM_LOCATIONS; i++) {
                array[i] = 0.0;
            }
            for (String s : list) {
                String[] split = s.split(";"); // we used the ';' instead of "-" because the Double.toString() may contain "-"
                String loc = split[0];
                array[Integer.parseInt(loc) - 1] = Double.valueOf(split[1]);
            }
            StringBuilder s = new StringBuilder();
            for (int i = 0, arrayLength = array.length; i < arrayLength; i++) {
                Double d = array[i];
                s.append(decimalFormat.format(d));
                if (i != arrayLength - 1) {
                    s.append("-"); // the CSVQuery2 expects a "-" instead of a ";"
                }
            }
            query2.add(new CSVQuery2(hour, avgTip, stdDevTip, payment, s.toString()));
        }
        return query2;
    }

    /**
     * Saves the result on Redis and on HDFS in CSV format.
     * @param result a list of CSVQuery2
     */
    @Override
    public void postProcessing(List<CSVQuery2> result) {
        // saving the result on hdfs...
        Query2.storeToCSVOnHDFS(result, this);
        System.out.println("================== written csv to HDFS =================");
        // ...and also on redis
        Query2.storeQuery2ToRedis(result);
        System.out.println("================== copied csv to local FS =================");
    }
}
