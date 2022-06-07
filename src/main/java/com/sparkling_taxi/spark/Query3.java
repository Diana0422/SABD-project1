package com.sparkling_taxi.spark;

import com.sparkling_taxi.bean.query3.*;
import com.sparkling_taxi.utils.Performance;
import com.sparkling_taxi.utils.Utils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import redis.clients.jedis.Jedis;
import scala.Tuple2;

import java.io.Serializable;
import java.util.*;

import static com.sparkling_taxi.utils.Const.*;

// docker cp backup/Query3.parquet namenode:/home/Query3.parquet
// hdfs dfs -put Query3.parquet /home/dataset-batch/Query3.parquet
public class Query3 {
    private static final int RANKING_SIZE = 5;

    public static void main(String[] args) {
        Query3 q = new Query3();
        q.preProcessing();
        List<Tuple2<DayLocationKey, Query3Result>> query3 = q.runQuery();
        q.postProcessing(query3);
    }

    public void preProcessing() {
        Utils.doPreProcessing(FILE_Q3, PRE_PROCESSING_TEMPLATE_Q3);
    }

    /**
     * Identify the top-5 most popular DOLocationIDs, indicating for each one:
     * - the average number of passengers,
     * - the mean and standard deviation of fare_amount
     *
     * @return the query3 result list
     */
    public List<Tuple2<DayLocationKey, Query3Result>> runQuery() {
        try (SparkSession spark = SparkSession
                .builder()
                .master("spark://spark:7077")
                .appName("Query3")
                .getOrCreate();
             JavaSparkContext jc = JavaSparkContext.fromSparkContext(spark.sparkContext())
        ) {
            spark.sparkContext().setLogLevel("WARN");
            List<Tuple2<DayLocationKey, Query3Result>> query3 = Performance.measure("Query3 - file4", () -> mostPopularDestinationWithTrueStdDev(spark, FILE_Q3));

            List<Tuple2<Integer, Tuple2<DayLocationKey, Query3Result>>> query3WithRank = new ArrayList<>();
            int j = 1;
            for (Tuple2<DayLocationKey, Query3Result> query3Result : query3) {
                query3WithRank.add(new Tuple2<>(j, query3Result));
                j++;
            }

            JavaRDD<CSVQuery3> result = jc.parallelize(query3WithRank)
                    // sometimes spark produces two partitions but the output file is small,
                    // so we force it to use only 1 partition
                    .repartition(1)
                    .map(v1 -> new CSVQuery3(v1._1, v1._2._1, v1._2._2)) // The rank is automatically incremented...: cambiare rank
                    .cache();

            // save to csv
            spark.createDataFrame(result, CSVQuery3.class)
                    .select("rank", "trips", "location", "meanPassengers", "meanFareAmount", "stDevFareAmount") // to set the correct order of columns!
                    .write()
                    .mode("overwrite")
                    .option("header", true)
                    .option("delimiter", ";")
                    .csv(OUT_DIR_Q3);


            System.out.println("=========== Ranking =============");
            for (int i = 0, collectSize = query3.size(); i < collectSize; i++) {
                Tuple2<DayLocationKey, Query3Result> res = query3.get(i);
                System.out.println((i + 1) + ") " + res.toString());
            }
            return query3;
        }
    }

    public static List<Tuple2<DayLocationKey, Query3Result>> mostPopularDestinationWithTrueStdDev(SparkSession spark, String file) {
        // Every element in the PairRdd contains: (DOLocationID, (1, passengers, fare_amount))
        // the "1" is used to count the occurrence of trips in the location
        Dataset<Row> parquet = spark.read().parquet(file);
        return parquet
                .as(Encoders.bean(Query3Bean.class))
                .toJavaRDD()
                // mapping the entire row to a new tuple with only the useful fields (and a 1 to count)
                .mapToPair(bean -> new Tuple2<>(new DayLocationKey(bean.getTpep_dropoff_datetime(), bean.getDOLocationID()), new Query3Calc(1, bean)))
                // reduceByKey sums the counter, the passengers, the fare_amount and square_fare_amount in order to compute the mean and stdev in the next method in the chain
                // DayLocationKey, Query3Calc{}
                .reduceByKey(Query3Calc::sumWith)
                // Query3Result: trips, location, avgPassengers, avgFareAmount, stdevFareAmount
                .mapValues(Query3Result::new)
                // Order descending by number of taxi_rides to each location
                // Keep only top-5 locations.
                .sortByKey(false)
                .takeOrdered(RANKING_SIZE, TupleComparator.INSTANCE);
        // To avoid "Task is not serializable" Error, we need a Serializable Comparator (Query3Comparator)
    }

    public void postProcessing(List<Tuple2<DayLocationKey, Query3Result>> query3) {
        try (Jedis jedis = new Jedis("redis://redis:6379")) {
            for (int i = 0, query3Size = query3.size(); i < query3Size; i++) {
                Tuple2<DayLocationKey, Query3Result> q = query3.get(i);
                HashMap<String, String> m = new HashMap<>();
                m.put("Rank", String.valueOf(i + 1));
                m.put("Day", q._1.getDay());
                m.put("LocationID", q._1.getDestination().toString()); // TODO: mappare con location string
                m.put("Trips Count", q._2.getTrips().toString());
                m.put("Avg Passengers", q._2.getMeanPassengers().toString());
                m.put("Avg Fare Amount", q._2.getMeanFareAmount().toString());
                m.put("Stdev Fare Amount", q._2.getStDevFareAmount().toString());
                jedis.hset(String.valueOf(i + 1), m);
            }
        }
    }

    private static class TupleComparator implements Serializable, Comparator<Tuple2<DayLocationKey, Query3Result>>{
        public static TupleComparator INSTANCE = new TupleComparator();

        @Override
        public int compare(Tuple2<DayLocationKey, Query3Result> o1, Tuple2<DayLocationKey, Query3Result> o2) {
            return Query3Result.Query3Comparator.INSTANCE.compare(o1._2, o2._2);
        }
    }
}
