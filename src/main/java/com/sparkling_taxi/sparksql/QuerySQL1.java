package com.sparkling_taxi.sparksql;

import com.sparkling_taxi.bean.query1.CSVQuery1;
import com.sparkling_taxi.utils.Performance;
import com.sparkling_taxi.utils.Utils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import redis.clients.jedis.Jedis;

import java.util.HashMap;
import java.util.List;

import static com.sparkling_taxi.utils.Const.*;
import static org.apache.spark.sql.functions.*;

public class QuerySQL1 {
    public static void main(String[] args) {
        QuerySQL1 q1 = new QuerySQL1();
        q1.preProcessing(); // NiFi pre-processing
        List<CSVQuery1> csvQuery1Dataset = q1.runQuery();
        q1.postProcessing(csvQuery1Dataset); // Save output to Redis
    }


    private void preProcessing() {
        Utils.doPreProcessing(FILE_Q1, PRE_PROCESSING_TEMPLATE_Q1);
    }

    public List<CSVQuery1> runQuery() {
        SparkSession spark = SparkSession
                .builder()
                .master("spark://spark:7077")
                .appName("QuerySQL1")
                .getOrCreate();
        spark.sparkContext().setLogLevel("WARN");

        spark.read().parquet(FILE_Q1)
                .toDF("pickup", "dropoff", "passengers", "tip", "toll", "total")
                .withColumn("month", month(col("dropoff")))
                .withColumn("year", year(col("dropoff")))
                .withColumn("tip_toll_ratio", expr("tip / (total - toll) "))
                .createOrReplaceTempView("query1");

        String query1 = "SELECT year, month, avg(passengers) as avgPassengers, avg(tip_toll_ratio) as avgRatio " +
                        "FROM query1 " +
                        "GROUP BY year, month " +
                        "ORDER BY year, month ";
        List<CSVQuery1> result = Performance.measure("SQL Query 1", () -> {
            Dataset<Row> sql = spark.sql(query1).cache();
            sql.show();
            return sql.as(Encoders.bean(CSVQuery1.class)).collectAsList();
        });

        spark.close();
        return result;
    }

    public void postProcessing(List<CSVQuery1> result) {
        try (Jedis jedis = new Jedis("redis://redis:6379")) {
            for (CSVQuery1 b : result) {
                HashMap<String, String> m = new HashMap<>();
                m.put("Year / Month", b.getYearMonth());
                m.put("Avg Passengers", String.valueOf(b.getAvgPassengers()));
                m.put("Avg Ratio", String.valueOf(b.getAvgRatio()));
                jedis.hset(b.getYearMonth(), m);
            }
        }
    }
}
