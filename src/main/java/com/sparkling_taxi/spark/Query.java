package com.sparkling_taxi.spark;

import com.sparkling_taxi.bean.QueryResult;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

import java.util.List;

public abstract class Query<T extends QueryResult> {
    protected final SparkSession spark;
    protected final JavaSparkContext jc;

    public Query(){
        spark = SparkSession
                .builder()
                .master("spark://spark:7077")
                .appName(this.getClass().getSimpleName()) // Query1, Query2, Query3
                .getOrCreate();
        jc = JavaSparkContext.fromSparkContext(spark.sparkContext());
        spark.sparkContext().setLogLevel("WARN");
    }

    public void closeSession() {
        if (jc != null) jc.close();
        if (spark != null) spark.close();
    }

    public abstract void preProcessing();
    public abstract List<T> processing();
    public abstract void postProcessing(List<T> queryResultList);
}
