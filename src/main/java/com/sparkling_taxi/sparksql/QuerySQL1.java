package com.sparkling_taxi.sparksql;

import com.sparkling_taxi.bean.query1.CSVQuery1;
import com.sparkling_taxi.spark.Query;
import com.sparkling_taxi.spark.Query1;
import com.sparkling_taxi.evaluation.Performance;
import com.sparkling_taxi.utils.Utils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;

import java.util.List;

import static com.sparkling_taxi.utils.Const.FILE_Q1;
import static com.sparkling_taxi.utils.Const.PRE_PROCESSING_TEMPLATE_Q1;
import static org.apache.spark.sql.functions.*;

public class QuerySQL1 extends Query<CSVQuery1> {
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

    public void preProcessing() {
        Utils.doPreProcessing(FILE_Q1, PRE_PROCESSING_TEMPLATE_Q1);
    }

    public List<CSVQuery1> processing() {
        System.out.println("======================= Running " + this.getClass().getSimpleName() + "=======================");
        spark.read().parquet(FILE_Q1)
                .toDF("dropoff", "tip", "toll", "total", "payment_type") // already removed other payment types on NiFi
                .withColumn("month", month(col("dropoff")))
                .withColumn("year", year(col("dropoff")))
                .withColumn("tip_toll_ratio", expr("tip / (total - toll) "))
                .drop("payment_type")
                .createOrReplaceTempView("query1");

        String query1 = "SELECT year, month, avg(tip_toll_ratio) as avgRatio, count(*) as count " +
                        "FROM query1 " +
                        "GROUP BY year, month " +
                        "ORDER BY year, month ";

        Dataset<Row> sql = spark.sql(query1);
        return sql.as(Encoders.bean(CSVQuery1.class)).collectAsList();
    }

    public void postProcessing(List<CSVQuery1> result) {
        Query1.storeToCSVOnHDFS(result, this);
        System.out.println("================== written csv to HDFS =================");

        Query1.storeQuery1ToRedis(result);
        System.out.println("================== copied csv to local FS =================");
    }
}
