package com.sparkling_taxi.spark;

import com.sparkling_taxi.bean.QueryResult;
import com.sparkling_taxi.utils.FileUtils;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

import java.util.List;
import java.util.function.Consumer;

public abstract class Query<T extends QueryResult> {
    protected final SparkSession spark;
    protected final JavaSparkContext jc;

    protected boolean forcePreprocessing;

    public Query() {
        spark = SparkSession
                .builder()
                .master("spark://spark:7077")
                .appName(this.getClass().getSimpleName()) // Query1, Query2, Query3
                .getOrCreate();
        jc = JavaSparkContext.fromSparkContext(spark.sparkContext());
        spark.sparkContext().setLogLevel("WARN");
        this.forcePreprocessing = false;
    }

    public Query(SparkSession s) {
        this.spark = s;
        jc = JavaSparkContext.fromSparkContext(s.sparkContext());
        this.forcePreprocessing = false;
        s.sparkContext().setLogLevel("WARN");
    }

    public Query(boolean forcePreprocessing, SparkSession s) {
        this.spark = s;
        jc = JavaSparkContext.fromSparkContext(s.sparkContext());
        this.forcePreprocessing = forcePreprocessing;
        s.sparkContext().setLogLevel("WARN");
    }

    public void closeSession() {
        if (jc != null) jc.close();
        if (spark != null) spark.close();
    }

    /**
     * Does the preprocessing with using a fixed NiFi template, if the input file for processing is not already present.
     * If the field forcePreprocessing is true, the preprocessing is always done.
     * It also downloads the dataset, but only if they are not already downloaded on HDFS.
     */
    public abstract void preProcessing();

    public abstract List<T> processing();

    /**
     * Implements the post processing for the query
     * @param queryResultList a list of queryResults
     */
    public abstract void postProcessing(List<T> queryResultList);

    public void copyAndRenameOutput(String outHDFS, String resultDir) {
        String newName = this.getClass().getSimpleName() + ".csv";
        String name = FileUtils.getFirstFileStartWithHDFS(outHDFS, "part-").orElse(newName);
        FileUtils.copyFromHDFS(outHDFS + "/" + name, resultDir + "/" + newName);
    }

    public boolean isForcePreprocessing() {
        return forcePreprocessing;
    }

    public void setForcePreprocessing(boolean forcePreprocessing) {
        this.forcePreprocessing = forcePreprocessing;
    }
}
