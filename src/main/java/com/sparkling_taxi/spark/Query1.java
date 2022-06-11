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
     * Average calculation on a monthly basis and on a subset of values:
     * - avg passengers
     * - avg tip/(total amount -toll amount)
     *
     * @param spark the initialized SparkSession
     * @param file  the input file
     * @return List of computed means
     */
    public List<Query1Result> multiMonthMeans(SparkSession spark, String file) {
        return spark.read().parquet(file)
                // Converts the typed Dataset<Row> to Dataset<Query1Bean>
                .as(Encoders.bean(Query1Bean.class))
                .toJavaRDD()
                .mapToPair(q1 -> new Tuple2<>(new YearMonthKey(q1.getTpep_dropoff_datetime()), new Query1Calc(1, q1)))
                /* after mapToPair: ((month, year), (1, passengers, ...)) */
                .reduceByKey(Query1Calc::sumWith)
                /* after reduceByKey: ((month, year), (count, sum_passengers, ...)) */
                .map(Query1Result::new)// Query1Result computes means inside the constructor
                /* after mapValues: ((month, year), (passengers_mean, other_mean...)) */
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

    public List<Query1Result> processing() {
        System.out.println("======================= Running " + this.getClass().getSimpleName() + " =======================");
        return multiMonthMeans(spark, FILE_Q1);
    }

    public void preProcessing() {
        Utils.doPreProcessing(FILE_Q1, PRE_PROCESSING_TEMPLATE_Q1, forcePreprocessing);
    }


    public void postProcessing(List<Query1Result> query1) {
        List<CSVQuery1> csvListResult = jc.parallelize(query1)
                // sometimes spark produces two partitions (two output files) but the output has only 3 lines,
                // so we force it to use only 1 partition
                .repartition(1)
                .map(CSVQuery1::new)
                .collect();

        storeToCSVOnHDFS(csvListResult, this);
        System.out.println("================== written csv to HDFS =================");
        // REDIS
        storeQuery1ToRedis(csvListResult);
        System.out.println("================== copied csv to local FS =================");

    }

    public static <T extends QueryResult> void storeToCSVOnHDFS(List<CSVQuery1> query1CsvList, Query<T> q) {
        // Dataframe is NOT statically typed, but uses less memory (GC) than dataset
        DataFrameWriter<Row> finalResult = q.spark.createDataFrame(query1CsvList, CSVQuery1.class)
                .select("year", new String[]{"month", "avgRatio", "count"}) // to set the correct order of columns!
                .write()
                .mode("overwrite")
                .option("header", true)
                .option("delimiter", ";");

        finalResult.csv(OUT_HDFS_URL_Q1);

        q.copyAndRenameOutput(OUT_HDFS_URL_Q1, RESULT_DIR1);
    }

    public static void storeQuery1ToRedis(List<CSVQuery1> csvListResult) {
        try (Jedis jedis = new Jedis("redis://redis:6379")) {
            for (CSVQuery1 t : csvListResult) {
                HashMap<String, String> m = new HashMap<>();
                m.put("Year / Month", t.getYearMonth());
                m.put("Avg Ratio", String.valueOf(t.getAvgRatio()));
                m.put("Count", String.valueOf(t.getCount()));
                jedis.hset(t.getYearMonth(), m);
            }
        }
        System.out.println("================= Stored on REDIS =================");
    }
}