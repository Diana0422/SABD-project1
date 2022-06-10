package com.sparkling_taxi.sparksql;

import com.sparkling_taxi.bean.query1.CSVQuery1;
import com.sparkling_taxi.bean.query2.CSVQuery2;
import com.sparkling_taxi.spark.Query;
import com.sparkling_taxi.spark.Query1;
import com.sparkling_taxi.spark.Query2;
import com.sparkling_taxi.utils.Utils;

import java.util.ArrayList;
import java.util.List;

import static com.sparkling_taxi.utils.Const.*;
import static org.apache.spark.sql.functions.*;

public class QuerySQL2 extends Query<CSVQuery2> {
    @Override
    public void preProcessing() {
        Utils.doPreProcessing(FILE_Q2, PRE_PROCESSING_TEMPLATE_Q2);
    }

    @Override
    public List<CSVQuery2> processing() {
        System.out.println("======================= Running " + this.getClass().getSimpleName() + " =======================");
//        spark.read().parquet(FILE_Q2)
//                .toDF("dropoff", "tip", "toll", "total", "payment_type") // already removed other payment types on NiFi
//                .withColumn("month", month(col("dropoff")))
//                .withColumn("year", year(col("dropoff")))
//                .withColumn("tip_toll_ratio", expr("tip / (total - toll) "))
//                .drop("payment_type")
//                .createOrReplaceTempView("query1");
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
