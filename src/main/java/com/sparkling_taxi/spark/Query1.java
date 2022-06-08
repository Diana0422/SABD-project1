package com.sparkling_taxi.spark;


import com.sparkling_taxi.bean.query1.*;
import com.sparkling_taxi.utils.Performance;
import com.sparkling_taxi.utils.Utils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.storage.StorageLevel;
import redis.clients.jedis.Jedis;
import scala.Tuple2;

import java.util.HashMap;
import java.util.List;

import static com.sparkling_taxi.utils.Const.*;

// docker cp backup/Query1.parquet namenode:/home/Query1.parquet
// hdfs dfs -get Query1.parquet /home/dataset-batch/Query1.parquet /home/Query1.parquet
public class Query1 extends Query<Query1Result> {

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
        System.out.println("======================= Query 1 =========================");
        // TODO: per migliorare le performance, su NiFi unire le tre colonne toll_amount, tip_amount, total_amount
        //  in una sola colonna ratio con il calcolo già effettuato!
        return spark.read().parquet(file)
                // Converts the typed Dataset<Row> to Dataset<Query1Bean>
                .as(Encoders.bean(Query1Bean.class))
                .toJavaRDD()
                .mapToPair(q1 -> new Tuple2<>(new YearMonthKey(q1.getTpep_dropoff_datetime()), new Query1Calc(1, q1)))
                /* after mapToPair: ((month, year), (1, passengers, ...)) */
                .reduceByKey(Query1Calc::sumWith)
                /* after reduceByKey: ((month, year), (count, sum_passengers, ...)) */
                .map(Query1Result::new)// Query1Result computes means inside the constructor
                .cache()
                /* after mapValues: ((month, year), (passengers_mean, other_mean...)) */
                .collect();
    }

    public Query1(){
        super();
    }

    public static void main(String[] args) {
        Query1 q = new Query1();
        q.preProcessing();
        List<Query1Result> query1 = q.processing();
        q.postProcessing(query1);
        q.closeSession();
    }

    public List<Query1Result> processing() {
        return Performance.measure("Complete Query 1", () -> multiMonthMeans(spark, FILE_Q1));
    }

    public void preProcessing() {
        Utils.doPreProcessing(FILE_Q1, PRE_PROCESSING_TEMPLATE_Q1);
    }


    public void postProcessing(List<Query1Result> query1) {
        JavaRDD<CSVQuery1> csvListResult = jc.parallelize(query1)
                // sometimes spark produces two partitions (two output files) but the output has only 3 lines,
                // so we force it to use only 1 partition
                .repartition(1)
                .map(CSVQuery1::new)
                .cache();

        // Dataframe is NOT statically typed, but uses less memory (GC) than dataset
        spark.createDataFrame(csvListResult, CSVQuery1.class)
                .select("year",new String[]{"month", "avgRatio", "count"}) // to set the correct order of columns!
                .write()
                .mode("overwrite")
                .option("header", true)
                .option("delimiter", ";")
                .csv(OUT_DIR_Q1);

        System.out.println("================== written to HDFS =================");

        query1.forEach(System.out::println);

        // REDIS
        try (Jedis jedis = new Jedis("redis://redis:6379")) {
            for (CSVQuery1 t: csvListResult.collect()) {
                HashMap<String, String> m = new HashMap<>();
                m.put("Year / Month", t.getYearMonth());
                m.put("Avg Ratio", String.valueOf(t.getAvgRatio()));
                m.put("Count", String.valueOf(t.getCount()));
                jedis.hset(t.getYearMonth(), m);
            }
        }
    }
}