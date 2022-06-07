package com.sparkling_taxi.spark;

import com.sparkling_taxi.bean.query3.*;
import com.sparkling_taxi.utils.Performance;
import com.sparkling_taxi.utils.Utils;
import lombok.var;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.storage.StorageLevel;
import redis.clients.jedis.Jedis;
import scala.Tuple2;

import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static com.sparkling_taxi.utils.Const.*;

// docker cp backup/Query3.parquet namenode:/home/Query3.parquet
// hdfs dfs -put Query3.parquet /home/dataset-batch/Query3.parquet
public class Query3 {
    private static final int RANKING_SIZE = 5;

    public static void main(String[] args) {
        Query3 q = new Query3();
        q.preProcessing();
        List<CSVQuery3> query3 = q.runQuery();
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
    public List<CSVQuery3> runQuery() {
        try (SparkSession spark = SparkSession
                .builder()
                .master("spark://spark:7077")
                .appName("Query3")
                .getOrCreate();
             JavaSparkContext jc = JavaSparkContext.fromSparkContext(spark.sparkContext())
        ) {
            spark.sparkContext().setLogLevel("WARN");
            List<Query3Result> query3 = Performance.measure("Query3 - file4", () -> mostPopularDestinationWithStdDev(spark, FILE_Q3));

            List<CSVQuery3> query3WithRank = new ArrayList<>();
            int j = 0;
            List<String> locations = new ArrayList<>();
            List<String> trips = new ArrayList<>();
            List<Double> meanPassengers = new ArrayList<>();
            List<Double> meanFareAmounts = new ArrayList<>();
            List<Double> stdDevFareAmounts = new ArrayList<>();

            for (var query3Result : query3) {
                if (j % RANKING_SIZE == 0 && j != 0) {
                    query3WithRank.add(new CSVQuery3(query3Result.getDay(), locations, trips, meanPassengers, meanFareAmounts, stdDevFareAmounts));
                    locations = new ArrayList<>();
                    trips = new ArrayList<>();
                    meanPassengers = new ArrayList<>();
                    meanFareAmounts = new ArrayList<>();
                    stdDevFareAmounts = new ArrayList<>();
                }
                locations.add(query3Result.getLocation().toString());
                trips.add(query3Result.getTrips().toString());
                meanPassengers.add(query3Result.getMeanPassengers());
                meanFareAmounts.add(query3Result.getMeanFareAmount());
                stdDevFareAmounts.add(query3Result.getStDevFareAmount());
                j++;
            }

            JavaRDD<CSVQuery3> result = jc.parallelize(query3WithRank)
                    .repartition(1);

            spark.createDataFrame(result, CSVQuery3.class)
                    .select("day",
                            "location1",
                            "location2",
                            "location3",
                            "location4",
                            "location5",
                            "trips1",
                            "trips2",
                            "trips3",
                            "trips4",
                            "trips5",
                            "meanPassengers1",
                            "meanPassengers2",
                            "meanPassengers3",
                            "meanPassengers4",
                            "meanPassengers5",
                            "meanFareAmount1",
                            "meanFareAmount2",
                            "meanFareAmount3",
                            "meanFareAmount4",
                            "meanFareAmount5",
                            "stDevFareAmount1",
                            "stDevFareAmount2",
                            "stDevFareAmount3",
                            "stDevFareAmount4",
                            "stDevFareAmount5")
                    .write()
                    .mode("overwrite")
                    .option("header", true)
                    .option("delimiter", ";")
                    .csv(OUT_DIR_Q3);
            return query3WithRank;
        }
    }

    public static List<Query3Result> mostPopularDestinationWithStdDev(SparkSession spark, String file) {
        // Every element in the PairRdd contains: (DOLocationID, (1, passengers, fare_amount))
        // the "1" is used to count the occurrence of trips in the location
        Dataset<Row> parquet = spark.read().parquet(file);
        JavaRDD<Query3Bean> rdd = parquet.as(Encoders.bean(Query3Bean.class))
                .toJavaRDD();

        JavaPairRDD<DayLocationKey, Query3Calc> mapping = rdd.mapToPair(bean -> new Tuple2<>(new DayLocationKey(bean.getTpep_dropoff_datetime(), bean.getDOLocationID()), new Query3Calc(1, bean)));
        JavaPairRDD<DayLocationKey, Query3Calc> reduced = mapping.reduceByKey(Query3Calc::sumWith);// (DayLocationKey, Query3Calc(cose sommate));
        reduced.persist(StorageLevel.MEMORY_AND_DISK());

        JavaPairRDD<String, Tuple2<Long, Query3Calc>> newPair = reduced.mapToPair(pair -> new Tuple2<>(pair._1.getDay(), new Tuple2<>(pair._1.getDestination(), pair._2)));
        JavaPairRDD<String, Iterable<Tuple2<Long, Query3Calc>>> grouped = newPair.groupByKey(); // (day, [ID, Query3Calc()..])
        JavaPairRDD<String, Tuple2<Long, Query3Calc>> top5 = grouped.flatMapValues(t -> {
            List<Tuple2<Long, Query3Calc>> list = StreamSupport.stream(t.spliterator(), false)
                    .collect(Collectors.toList());

            ArrayList<Tuple2<Long, Query3Calc>> top = new ArrayList<>();
            for (int i = 0; i < RANKING_SIZE; i++) {
                Optional<Tuple2<Long, Query3Calc>> max = list.stream().max(Comparator.comparingDouble(o -> o._2.getCount()));
                if (max.isPresent()) {
                    list.remove(max.get());
                    top.add(max.get());
                }
            }

            return top.iterator();
        });

        return top5.map(resultPair -> new Query3Result(resultPair._1, resultPair._2._1, resultPair._2._2))
                .collect();
    }

    public void postProcessing(List<CSVQuery3> query3) {
        try (Jedis jedis = new Jedis("redis://redis:6379")) {
            for (CSVQuery3 q : query3) {
                HashMap<String, String> m = new HashMap<>();
                m.put("day", q.getDay());
                m.put("location1", q.getLocation1());
                m.put("location2", q.getLocation2());
                m.put("location3", q.getLocation3());
                m.put("location4", q.getLocation4());
                m.put("location5", q.getLocation5());
                m.put("trips1", q.getTrips1());
                m.put("trips2", q.getTrips2());
                m.put("trips3", q.getTrips3());
                m.put("trips4", q.getTrips4());
                m.put("trips5", q.getTrips5());
                m.put("meanPassengers1", q.getMeanPassengers1());
                m.put("meanPassengers2", q.getMeanPassengers2());
                m.put("meanPassengers3", q.getMeanPassengers3());
                m.put("meanPassengers4", q.getMeanPassengers4());
                m.put("meanPassengers5", q.getMeanPassengers5());
                m.put("meanFareAmount1", q.getMeanFareAmount1());
                m.put("meanFareAmount2", q.getMeanFareAmount2());
                m.put("meanFareAmount3", q.getMeanFareAmount3());
                m.put("meanFareAmount4", q.getMeanFareAmount4());
                m.put("meanFareAmount5", q.getMeanFareAmount5());
                m.put("stdDevFareAmount1", q.getStDevFareAmount1());
                m.put("stdDevFareAmount2", q.getStDevFareAmount2());
                m.put("stdDevFareAmount3", q.getStDevFareAmount3());
                m.put("stdDevFareAmount4", q.getStDevFareAmount4());
                m.put("stdDevFareAmount5", q.getStDevFareAmount5());
                jedis.hset(q.getDay(), m);
            }
        }
    }
}
