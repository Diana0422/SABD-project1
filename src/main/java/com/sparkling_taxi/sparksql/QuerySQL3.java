package com.sparkling_taxi.sparksql;
import com.sparkling_taxi.utils.Performance;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static com.sparkling_taxi.utils.Const.FILE_Q3;

public class QuerySQL3 {
    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .master("spark://spark:7077")
                .appName("QuerySQL3")
                .getOrCreate();
        spark.sparkContext().setLogLevel("WARN");

        Dataset<Row> parquet = spark.read().parquet(FILE_Q3);
        Dataset<Row> rowDataset = preprocessing(parquet);
        rowDataset.createOrReplaceTempView("query3");

        String sqlQuery = "SELECT location ,count(*) as trips, avg(passengers), stddev(fare_amount), avg(fare_amount)\n" +
                          "FROM query3\n" +
                          "GROUP BY location\n" +
                          "ORDER BY trips DESC\n" +
                          "LIMIT 5";

        Performance.measure("SQL Query 3", () -> {
            Dataset<Row> sql = spark.sql(sqlQuery);
            sql.show();
        });
        spark.close();
    }

    private static Dataset<Row> preprocessing(Dataset<Row> dataset) {
        return dataset.toDF("pickup", "dropoff", "passengers", "location", "payment_type", "fare_amount");
    }
}
