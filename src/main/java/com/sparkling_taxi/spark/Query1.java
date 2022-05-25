package com.sparkling_taxi.spark;


import com.sparkling_taxi.bean.Query1Bean;
import com.sparkling_taxi.bean.Query1Calc;
import com.sparkling_taxi.bean.Query1Result;
import com.sparkling_taxi.bean.YearMonth;
import com.sparkling_taxi.utils.Performance;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.io.File;
import java.util.List;

// docker cp backup/Query1.parquet namenode:/home/Query1.parquet
// hdfs dfs -get Query1.parquet /home/dataset-batch/Query1.parquet /home/Query1.parquet
public class Query1 {


    public static final String FILE_Q1 = "hdfs://namenode:9000/home/dataset-batch/Query1.parquet";
    public static final String OUT_DIR = "hdfs://namenode:9000/home/dataset-batch/output-query1";
    public static final String DIR_TIMESTAMP = "hdfs://namenode:9000/home/dataset-batch/timestamp";
    public static final int PASSENGER_COUNT_COL = 2;
    public static final int DROP_OUT_COL = 1;

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
     * @param spark the initialized SparkSession
     * @param file the input file
     * @return List of computed means
     */
    public static List<Tuple2<YearMonth, Query1Result>> multiMonthMeans(SparkSession spark, String file) {
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
        // TODO: chiamare NiFi da qui

        // Must install WINUTILS.EXE for Windows.
        windowsCheck();

        SparkSession spark = SparkSession
                .builder()
                .master("spark://spark:7077")
                .appName("Query1")
                .getOrCreate();
        spark.sparkContext().setLogLevel("WARN");

        List<Tuple2<YearMonth, Query1Result>> query1 = Performance.measure("Complete Query 1", () -> multiMonthMeans(spark, FILE_Q1));

        JavaSparkContext sc = JavaSparkContext.fromSparkContext(spark.sparkContext());
        JavaRDD<Tuple2<YearMonth, Query1Result>> result = sc.parallelize(query1)
                // sometimes spark produces two partitions but the output file is small,
                // so we force it to use only 1 partition
                .repartition(1)
                .cache();
        result.saveAsTextFile(OUT_DIR);
        System.out.println("================== written to HDFS =================");

        result.collect().forEach(System.out::println);

        sc.close();
        spark.close();
    }
}