package com.sparkling_taxi.spark;


import com.sparkling_taxi.bean.*;
import com.sparkling_taxi.nifi.NifiTemplateInstance;
import com.sparkling_taxi.utils.Performance;
import com.sparkling_taxi.utils.Utils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import redis.clients.jedis.Jedis;
import scala.Tuple2;

import java.io.File;
import java.util.HashMap;
import java.util.List;

// docker cp backup/Query1.parquet namenode:/home/Query1.parquet
// hdfs dfs -get Query1.parquet /home/dataset-batch/Query1.parquet /home/Query1.parquet
public class Query1 {

    public static final String PRE_PROCESSING_TEMPLATE_Q1 = "/home/templates/preprocessing_query1.xml";
    public static final String FILE_Q1 = "hdfs://namenode:9000/home/dataset-batch/Query1.parquet";
    public static final String OUT_DIR = "hdfs://namenode:9000/home/dataset-batch/output-query1";

    private NifiTemplateInstance n;

    /**
     * @return true if on windows is installed C:\\Hadoop\\hadoop-2.8.1\\bin\\WINUTILS.EXE
     */
    private static boolean windowsCheck() {
        if (System.getProperty("os.name").equals("windows")) {
            System.out.println("Hi");
            if (new File("C:\\Hadoop\\hadoop-2.8.1").exists()) {
                System.setProperty("hadoop.home.dir", "C:\\Hadoop");
            } else {
                System.out.println("Install WINUTIL.EXE from and unpack it in C:\\Windows");
                return false;
            }
        }
        return true;
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
    public List<Tuple2<YearMonth, Query1Result>> multiMonthMeans(SparkSession spark, String file) {
        System.out.println("======================= Query 1 =========================");
        return spark.read().parquet(file)
                // Converts the typed Dataset<Row> to Dataset<Query1Bean>
                .as(Encoders.bean(Query1Bean.class))
                .toJavaRDD()
                .mapToPair(q1 -> new Tuple2<>(new YearMonth(q1.getTpep_dropoff_datetime()), new Query1Calc(1, q1)))
                /* after mapToPair: ((month, year), (1, passengers, ...)) */
                .reduceByKey(Query1Calc::sumWith)
                /* after reduceByKey: ((month, year), (count, sum_passengers, ...)) */
                .mapValues(Query1Result::new)// Query1Result computes means inside the constructor
                /* after mapValues: ((month, year), (passengers_mean, other_mean...)) */
                .sortByKey(true)
                .collect();
    }

    public static void main(String[] args) {
        Query1 q = new Query1();
        q.preProcessing();
        List<Tuple2<YearMonth, Query1Result>> query1 = q.runQuery();
        q.postProcessing(query1);
    }

    public List<Tuple2<YearMonth, Query1Result>> runQuery() {
        SparkSession spark = SparkSession
                .builder()
                .master("spark://spark:7077")
                .appName("Query1")
                .getOrCreate();
        spark.sparkContext().setLogLevel("WARN");

        List<Tuple2<YearMonth, Query1Result>> query1 = Performance.measure("Complete Query 1", () -> multiMonthMeans(spark, FILE_Q1));

        JavaSparkContext sc = JavaSparkContext.fromSparkContext(spark.sparkContext());
        JavaRDD<CSVQuery1> result = sc.parallelize(query1)
                // sometimes spark produces two partitions but the output file is small,
                // so we force it to use only 1 partition
                .repartition(1)
                .map(v1 -> new CSVQuery1(v1._1, v1._2))
                .cache();


        // Dataframe is NOT statically typed, but uses less memory (GC) than dataset
        spark.createDataFrame(result, CSVQuery1.class)
                .select("year", "month", "avgPassengers", "avgRatio") // to set the correct order of columns!
                .write()
                .mode("overwrite")
                .option("header", true)
                .option("delimiter", ";")
                .csv(OUT_DIR);

        postProcessing(query1);


        // result.saveAsTextFile(OUT_DIR);
        System.out.println("================== written to HDFS =================");

        query1.forEach(System.out::println);

        sc.close();
        spark.close();

        return query1;
    }

    public void preProcessing() {
        Utils.doPreProcessing(FILE_Q1, PRE_PROCESSING_TEMPLATE_Q1);
    }


    public void postProcessing(List<Tuple2<YearMonth, Query1Result>> query1) {
        //TODO: grafana redis interaction
        Jedis jedis = new Jedis("redis://redis:6379");
        for (Tuple2<YearMonth, Query1Result> t : query1) {
            HashMap<String, String> m = new HashMap<>();
            m.put("Year / Month",t._1().toString());
            m.put("Avg Passengers", String.valueOf(t._2.getAvgPassengers()));
            m.put("Avg Ratio", String.valueOf(t._2.getAvgRatio()));
            jedis.hset(t._1().toString(), m);
        }
    }
}