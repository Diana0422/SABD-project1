package com.sparkling_taxi.spark;

import com.sparkling_taxi.bean.QueryResult;
import com.sparkling_taxi.bean.query3.*;
import com.sparkling_taxi.utils.Performance;
import com.sparkling_taxi.utils.Utils;
import lombok.var;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.DataFrameWriter;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import redis.clients.jedis.Jedis;
import scala.Tuple2;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static com.sparkling_taxi.utils.Const.*;

// docker cp backup/Query3.parquet namenode:/home/Query3.parquet
// hdfs dfs -put Query3.parquet /home/dataset-batch/Query3.parquet
public class Query3 extends Query<Query3Result> {

    public static void main(String[] args) {
        Query3 q = new Query3();
        q.preProcessing();
        List<Query3Result> query3 = Performance.measure(q::processing);
        q.postProcessing(query3);
        q.closeSession();
    }

    public Query3() {
        super();
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
    public List<Query3Result> processing() {
        System.out.println("======================= Running " + this.getClass().getSimpleName() + "=======================");
        return mostPopularDestinationWithStdDev(spark, FILE_Q3);
    }

    public static List<Query3Result> mostPopularDestinationWithStdDev(SparkSession spark, String file) {
        // Every element in the PairRdd contains: (DOLocationID, (1, passengers, fare_amount))
        // the "1" is used to count the occurrence of trips in the location

        return spark.read().parquet(file)
                .as(Encoders.bean(Query3Bean.class))
                .toJavaRDD()
                .mapToPair(bean -> new Tuple2<>(new DayLocationKey(bean.getTpep_dropoff_datetime(), bean.getDOLocationID()), new Query3Calc(1, bean)))
                .reduceByKey(Query3Calc::sumWith) // (DayLocationKey, Query3Calc(cose sommate))
                .mapToPair(pair -> new Tuple2<>(pair._1.getDay(), new Tuple2<>(pair._1.getDestination(), pair._2)))
                .groupByKey() // (day, [ID, Query3Calc()..])
                .flatMapValues(t -> {
                    List<Tuple2<Long, Query3Calc>> list = StreamSupport.stream(t.spliterator(), false).collect(Collectors.toList());
                    ArrayList<Tuple2<Long, Query3Calc>> top = new ArrayList<>();
                    for (int i = 0; i < RANKING_SIZE; i++) {
                        Optional<Tuple2<Long, Query3Calc>> max = list.stream().max(Comparator.comparingDouble(o -> o._2.getCount()));
                        if (max.isPresent()) {
                            list.remove(max.get());
                            top.add(max.get());
                        }
                    }
                    return top.iterator();
                })
                .map(resultPair -> new Query3Result(resultPair._1, resultPair._2._1, resultPair._2._2))
                .collect();
    }

    public void postProcessing(List<Query3Result> query3) {
        Map<String, String> zones = spark.read()
                .option("header", false)
                .csv(ZONES_CSV)
                .toDF("LocationID", "Borough", "Zone", "service_zone")
                .drop("service_zone")
                .collectAsList()
                .stream()
                .map(row -> new Zone(row.getString(0), row.getString(1), row.getString(2)))
                .collect(Collectors.toMap(Zone::getId, Zone::zoneString));


        List<CSVQuery3> query3CsvList = new ArrayList<>();
        int j = 0;
        List<String> locations = new ArrayList<>();
        List<String> trips = new ArrayList<>();
        List<Double> meanPassengers = new ArrayList<>();
        List<Double> meanFareAmounts = new ArrayList<>();
        List<Double> stdDevFareAmounts = new ArrayList<>();

        for (var query3Result : query3) {
            if (j % RANKING_SIZE == 0 && j != 0) {
                query3CsvList.add(new CSVQuery3(query3Result.getDay(), locations, trips, meanPassengers, meanFareAmounts, stdDevFareAmounts));
                locations = new ArrayList<>();
                trips = new ArrayList<>();
                meanPassengers = new ArrayList<>();
                meanFareAmounts = new ArrayList<>();
                stdDevFareAmounts = new ArrayList<>();
            }
            locations.add(zones.get(query3Result.getLocation().toString()));
            trips.add(query3Result.getTrips().toString());
            meanPassengers.add(query3Result.getMeanPassengers());
            meanFareAmounts.add(query3Result.getMeanFareAmount());
            stdDevFareAmounts.add(query3Result.getStDevFareAmount());
            j++;
        }

        storeToCSVOnHDFS(query3CsvList, this);
        // REDIS
        storeQuery3ToRedis(query3CsvList);
    }

    public static <T extends QueryResult> void storeToCSVOnHDFS(List<CSVQuery3> query3CsvList, Query<T> q) {
        DataFrameWriter<Row> finalResult = q.spark.createDataFrame(query3CsvList, CSVQuery3.class)
                .select("day",
                        "location1", "location2", "location3", "location4", "location5",
                        "trips1", "trips2", "trips3", "trips4", "trips5",
                        "meanPassengers1", "meanPassengers2", "meanPassengers3", "meanPassengers4", "meanPassengers5",
                        "meanFareAmount1", "meanFareAmount2", "meanFareAmount3", "meanFareAmount4", "meanFareAmount5",
                        "stDevFareAmount1", "stDevFareAmount2", "stDevFareAmount3", "stDevFareAmount4", "stDevFareAmount5")
                .write()
                .mode("overwrite")
                .option("header", true)
                .option("delimiter", ";");

        finalResult.csv(OUT_HDFS_URL_Q3);
        System.out.println("================== written csv to HDFS =================");

        // also copies to local file system thanks to a docker volume
        q.copyAndRenameOutput(OUT_HDFS_URL_Q3, RESULT_DIR3);
        System.out.println("================== copied csv to local FS =================");

    }

    public static void storeQuery3ToRedis(List<CSVQuery3> query3CsvList) {
        try (Jedis jedis = new Jedis(REDIS_URL)) {
            for (CSVQuery3 q : query3CsvList) {
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
        System.out.println("================= Stored on REDIS =================");
    }

}
