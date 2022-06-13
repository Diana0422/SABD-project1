package com.sparkling_taxi.spark;


import com.sparkling_taxi.bean.QueryResult;
import com.sparkling_taxi.bean.query1.*;
import com.sparkling_taxi.evaluation.Performance;
import com.sparkling_taxi.utils.Utils;
import org.apache.spark.sql.DataFrameWriter;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import redis.clients.jedis.Jedis;
import scala.Tuple2;

import java.util.HashMap;
import java.util.List;

import static com.sparkling_taxi.utils.Const.*;

// docker cp backup/Query1.parquet namenode:/home/Query1.parquet
// hdfs dfs -get Query1.parquet /home/dataset-batch/Query1.parquet /home/Query1.parquet
public class Query1 extends Query<Query1Result> {

    public Query1(SparkSession s) {
        super(s);
    }

    public Query1(boolean b, SparkSession s) {
        super(b, s);
    }

    /**
     * Spark Query1:
     * Average calculation on a monthly basis of tip/(total amount -toll amount)
     * @param spark the initialized SparkSession
     * @param file  the input file
     * @return List of computed means
     */
    public List<Query1Result> query1(SparkSession spark, String file) {
        return spark.read().parquet(file)
                // Converts the typed Dataset<Row> to Dataset<Query1Bean>
                .as(Encoders.bean(Query1Bean.class))
                .toJavaRDD()
                .mapToPair(q1 -> new Tuple2<>(new YearMonthKey(q1.getTpep_dropoff_datetime()), new Query1Calc(1, q1)))
                /* after mapToPair: ((month, year), Query1Calc(count=1, ratio=tip_amount/(total-toll))) */
                .reduceByKey(Query1Calc::sumWith)
                /* after reduceByKey: ((month, year), Query1Calc(count=occurrences, ratio=sum_ratio) */
                .map(Query1Result::new)// Query1Result computes means inside the constructor using Query1Calc methods
                /* after map: (Query1Result(yearMonth=YearMonthKey(..), avgRatio=sum_ratio/count , count=occurrences )) */
                .collect();
    }

    public Query1() {
        super();
    }

    public static void main(String[] args) {
        Query1 q = new Query1();
        q.preProcessing();
        List<Query1Result> query1 = Performance.measure("Query completa 1 ", q::processing);
        q.postProcessing(query1);
        q.closeSession();
    }

    /**
     * Triggers the execution of the Query using Spark
     * @return a list of Query1Result instances
     */
    public List<Query1Result> processing() {
        System.out.println("======================= Running " + this.getClass().getSimpleName() + " =======================");
        return query1(spark, FILE_Q1);
    }

    /**
     * Triggers the execution of the preprocessing using NiFiAPI
     */
    public void preProcessing() {
        Utils.doPreProcessing(FILE_Q1, PRE_PROCESSING_TEMPLATE_Q1, forcePreprocessing);
    }


    /**
     * Stores the result of the query given in input on the HDFS and also stores the same data on
     * Redis (serving layer).
     * @param query1 a list of Query1Result instances (one for each row)
     */
    public void postProcessing(List<Query1Result> query1) {
        List<CSVQuery1> csvListResult = jc.parallelize(query1)
                .map(CSVQuery1::new)
                .collect();

        storeToCSVOnHDFS(csvListResult, this);
        System.out.println("================== written csv to HDFS =================");
        // REDIS
        storeQuery1ToRedis(csvListResult);
        System.out.println("================== copied csv to local FS =================");

    }

    /**
     * Stores the final output of the query given in input into the HDFS.
     * @param query1CsvList the list of CSVQuery1 instances to save in the HDFS as a CSV file
     * @param q the query executed
     * @param <T> the specific type of Query executed
     */
    public static <T extends QueryResult> void storeToCSVOnHDFS(List<CSVQuery1> query1CsvList, Query<T> q) {
        // Dataframe is NOT statically typed, but uses less memory (GC) than dataset
        DataFrameWriter<Row> finalResult = q.spark.createDataFrame(query1CsvList, CSVQuery1.class)
                .select("year", new String[]{"month", "avgRatio", "count"}) // to set the correct order of columns!
                .coalesce(1)
                // sometimes spark produces two partitions (two output files) but the output has only 3 lines,
                // so we force it to use only 1 partition
                .sort("year", "month")
                .write()
                .mode("overwrite")
                .option("header", true)
                .option("delimiter", ";");

        finalResult.csv(OUT_HDFS_URL_Q1);

        q.copyAndRenameOutput(OUT_HDFS_URL_Q1, RESULT_DIR1);
    }


    /**
     * Saves in Redis the final output ad a HashMap
     * @param csvListResult the list of CSVQuery1 output instances
     */
    public static void storeQuery1ToRedis(List<CSVQuery1> csvListResult) {
        try (Jedis jedis = new Jedis("redis://redis:6379")) {
            for (CSVQuery1 t : csvListResult) {
                HashMap<String, String> m = new HashMap<>();
                m.put("year_month", t.getYearMonth());
                m.put("avg_ratio", String.valueOf(t.getAvgRatio()));
                m.put("count", String.valueOf(t.getCount()));
                jedis.hset(t.getYearMonth(), m);//saves a HashMap for each instance with key YYYY/MM
            }
        }
        System.out.println("================= Stored on REDIS =================");
    }
}