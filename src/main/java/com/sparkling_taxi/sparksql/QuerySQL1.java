package com.sparkling_taxi.sparksql;

import com.sparkling_taxi.utils.Performance;
import com.sparkling_taxi.spark.Query1;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.*;

public class QuerySQL1 {
    public static void main(String[] args) {
        // TODO: chiamare NiFi da qui

        SparkSession spark = SparkSession
                .builder()
                .master("spark://spark:7077")
                .appName("QuerySQL1")
                .getOrCreate();
        spark.sparkContext().setLogLevel("WARN");

        Dataset<Row> parquet = spark.read().parquet(Query1.FILE_Q1);
        Dataset<Row> rowDataset = preprocessing(parquet);
        rowDataset.createOrReplaceTempView("query1");

        Dataset<Row> sql = Performance.measure("SQL Query 1", () -> spark.sql(
                "SELECT dropoff_month, avg(passengers) as avg_passengers, avg(tip_toll_ratio) as avg_ratio " +
                        "FROM query1 " +
                        "GROUP BY dropoff_month "));
        sql.show();
//
//        RelationalGroupedDataset relationalGroupedDataset = rowDataset.withColumn("dropoff_hour", month(col("dropoff")))
//                .withColumn("tip_toll_ratio", expr("tip / (total - toll) "))
//                .select(avg("passengers"), avg("tip"), avg("tip_toll_ratio"))
//                .groupBy(col("dropoff_hour"));

        //  System.out.println(relationalGroupedDataset.toString());

        spark.close();
    }

    private static Dataset<Row> preprocessing(Dataset<Row> dataset) {
        return dataset.toDF("pickup", "dropoff", "passengers", "tip", "toll", "total")
                .withColumn("dropoff_month", month(col("dropoff")))
                .withColumn("tip_toll_ratio", expr("tip / (total - toll) "));
    }
}
