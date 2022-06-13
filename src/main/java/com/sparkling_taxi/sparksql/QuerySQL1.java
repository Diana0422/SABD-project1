package com.sparkling_taxi.sparksql;

import com.sparkling_taxi.bean.query1.CSVQuery1;
import com.sparkling_taxi.spark.Query;
import com.sparkling_taxi.spark.Query1;
import com.sparkling_taxi.evaluation.Performance;
import com.sparkling_taxi.utils.Utils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.List;

import static com.sparkling_taxi.utils.Const.FILE_Q1;
import static com.sparkling_taxi.utils.Const.PRE_PROCESSING_TEMPLATE_Q1;
import static org.apache.spark.sql.functions.*;

public class QuerySQL1 extends Query<CSVQuery1> {
    public QuerySQL1(SparkSession s) {
        super(s);
    }

    public QuerySQL1(boolean forcePreprocessing, SparkSession s) {
        super(forcePreprocessing, s);
    }

    public static void main(String[] args) {
        QuerySQL1 q1 = new QuerySQL1();
        q1.preProcessing(); // NiFi pre-processing
        List<CSVQuery1> csvQuery1Dataset = Performance.measure(q1::processing);
        q1.postProcessing(csvQuery1Dataset); // Save output to Redis
        q1.closeSession();
    }

    public QuerySQL1() {
        super();
    }

    /**
     * Does the preprocessing with using a fixed NiFi template, if the input file for processing is not already present.
     * If the field forcePreprocessing is true, the preprocessing is always done.
     * It also downloads the dataset, but only if they are not already downloaded on HDFS.
     */
    public void preProcessing() {
        Utils.doPreProcessing(FILE_Q1, PRE_PROCESSING_TEMPLATE_Q1, forcePreprocessing);
    }

    /**
     * Implements the query1 processing with SparkSQL
     * @return a list of CSVQuery1
     */
    public List<CSVQuery1> processing() {
        System.out.println("======================= Running " + this.getClass().getSimpleName() + " =======================");

        // reads the unified parquet from hdfs, gets month and year from the timestamps and aggregates the tip/ (total - toll)
        spark.read().parquet(FILE_Q1)
                .toDF("dropoff", "tip", "toll", "total", "payment_type") // already removed other payment types on NiFi
                .withColumn("month", month(col("dropoff")))
                .withColumn("year", year(col("dropoff")))
                .withColumn("tip_toll_ratio", expr("tip / (total - toll) "))
                .drop("payment_type") // we don't need this
                .createOrReplaceTempView("query1");
        // the query to get the average proportion between tip and payed amount and the number of trips each year and month
        String query1 = "SELECT year, month, avg(tip_toll_ratio) as avgRatio, count(*) as count \n" +
                        "FROM query1 \n" +
                        "GROUP BY year, month \n" +
                        "ORDER BY year, month \n";

        Dataset<Row> sql = spark.sql(query1);
        // materializes the query with the collectAsList action
        return sql.as(Encoders.bean(CSVQuery1.class)).collectAsList();
    }

    /**
     * Saves the result on Redis and on HDFS in CSV format.
     * @param result a list of CSVQuery1
     */
    public void postProcessing(List<CSVQuery1> result) {
        // saving the result on hdfs...
        Query1.storeToCSVOnHDFS(result, this);
        System.out.println("================== written csv to HDFS =================");
        // ...and also on redis
        Query1.storeQuery1ToRedis(result);
        System.out.println("================== copied csv to local FS =================");
    }
}
