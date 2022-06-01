package com.sparkling_taxi.spark;

import com.sparkling_taxi.bean.query3.CSVQuery3;
import com.sparkling_taxi.bean.query3.Query3Bean;
import com.sparkling_taxi.bean.query3.Query3Calc;
import com.sparkling_taxi.bean.query3.Query3Result;
import com.sparkling_taxi.utils.Performance;
import com.sparkling_taxi.utils.Utils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.util.StatCounter;
import redis.clients.jedis.Jedis;
import scala.Tuple2;
import scala.Tuple3;

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

// docker cp backup/Query3.parquet namenode:/home/Query3.parquet
// hdfs dfs -put Query3.parquet /home/dataset-batch/Query3.parquet
public class Query3 {
    public static final String PRE_PROCESSING_TEMPLATE_Q3 = "/home/templates/preprocessing_query3.xml";
    public static final String FILE_Q3 = "hdfs://namenode:9000/home/dataset-batch/Query3.parquet";

    public static final String OUT_DIR_Q3 = "hdfs://namenode:9000/home/dataset-batch/output-query3";
    public static final int DO_LOC_COL = 3;
    public static final int PASSENGER_COUNT_COL = 2;
    public static final int FARE_AMOUNT_COL = 5;
    private static final int RANKING_SIZE = 5;

    public static void main(String[] args) {
        Query3 q = new Query3();
        q.preProcessing();
        List<Query3Result> query3 = q.runQuery();
        q.postProcessing(query3);
    }

    private void preProcessing() {
        Utils.doPreProcessing(FILE_Q3, PRE_PROCESSING_TEMPLATE_Q3);
    }

    /**
     * Identify the top-5 most popular DOLocationIDs, indicating for each one:
     * - the average number of passengers,
     * - the mean and standard deviation of fare_amount
     *
     * @return the query3 result list
     */
    public List<Query3Result> runQuery() {
        try (SparkSession spark = SparkSession
                .builder()
                .master("spark://spark:7077")
                .appName("Query3")
                .getOrCreate();
             JavaSparkContext jc = JavaSparkContext.fromSparkContext(spark.sparkContext())
        ) {
            spark.sparkContext().setLogLevel("WARN");
            List<Query3Result> query3 = Performance.measure("Query3 - file4", () -> mostPopularDestinationWithTrueStdDev(spark, FILE_Q3));

            List<Tuple2<Integer, Query3Result>> query3WithRank = new ArrayList<>();
            int j = 1;
            for (Query3Result query3Result : query3) {
                query3WithRank.add(new Tuple2<>(j, query3Result));
                j++;
            }

            JavaRDD<CSVQuery3> result = jc.parallelize(query3WithRank)
                    // sometimes spark produces two partitions but the output file is small,
                    // so we force it to use only 1 partition
                    .repartition(1)
                    .map(v1 -> new CSVQuery3(v1._1, v1._2)) // The rank is automatically incremented...: cambiare rank
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
                Query3Result res = query3.get(i);
                System.out.println((i + 1) + ") " + res.toString());
            }
            return query3;
        }
    }

    private static List<Query3Result> mostPopularDestinationWithTrueStdDev(SparkSession spark, String file) {
        // Every element in the PairRdd contains: (DOLocationID, (1, passengers, fare_amount))
        // the "1" is used to count the occurrence of trips in the location
        Dataset<Row> parquet = spark.read().parquet(file);
        return parquet
                .as(Encoders.bean(Query3Bean.class))
                .toJavaRDD()
                // mapping the entire row to a new tuple with only the useful fields (and a 1 to count)
                .mapToPair(bean -> new Tuple2<>(bean.getDOLocationID(), new Query3Calc(1, bean)))
                // reduceByKey sums the counter, the passengers, the fare_amount and square_fare_amount in order to compute the mean and stdev in the next method in the chain
                // Tuple2: (DOLocationID, Query3Calc[total_taxi_trips, passengers_sum, fare_amount_sum, fare_amount_square_sum])
                .reduceByKey(Query3Calc::sumWith)
                // Query3Result: trips, location, avgPassengers, avgFareAmount, stdevFareAmount
                .map(tup -> new Query3Result(tup._2.getCount(), // count
                        tup._1, // location
                        tup._2.computePassengerMean(),
                        tup._2.computeFareAmountMean(),
                        tup._2.computeFareAmountStdev()))
                // Order descending by number of taxi_rides to each location
                // Keep only top-5 locations.
                .takeOrdered(RANKING_SIZE, Query3Result.Query3Comparator.INSTANCE);
        // To avoid "Task is not serializable" Error, we need a Serializable Comparator (Query3Comparator)
    }

    public void postProcessing(List<Query3Result> query3) {
        try (Jedis jedis = new Jedis("redis://redis:6379")) {
            for (int i = 0, query3Size = query3.size(); i < query3Size; i++) {
                Query3Result q = query3.get(i);
                HashMap<String, String> m = new HashMap<>();
                m.put("Rank", String.valueOf(i + 1));
                m.put("Trips Count", q.getTrips().toString());
                m.put("LocationID", q.getLocation().toString()); // TODO: mappare con location string
                m.put("Avg Passengers", q.getMeanPassengers().toString());
                m.put("Avg Fare Amount", q.getMeanFareAmount().toString());
                m.put("Stdev Fare Amount", q.getStDevFareAmount().toString());
                jedis.hset(String.valueOf(i + 1), m);
            }
        }
    }

    private static void mostPopularDestinationWithStdDev(SparkSession spark, String file) {
        // Every element in the PairRdd contains: (DOLocationID, (1, passengers, fare_amount))
        // the "1" is used to count the occurrence of the taxi_rides to a specific destination
        List<Tuple2<Long, Tuple3<Long, Double, Double>>> take = spark.read().parquet(file)
                .toJavaRDD()
                // excluding the rows with at least one null value in the necessary columns
                .filter(row -> !(row.isNullAt(DO_LOC_COL) || row.isNullAt(PASSENGER_COUNT_COL) || row.isNullAt(FARE_AMOUNT_COL)))
                // mapping the entire row to a new tuple with only the useful fields (and a 1 to count)
                .mapToPair(row -> new Tuple2<>(row.getLong(DO_LOC_COL), row.getDouble(FARE_AMOUNT_COL)))
                .aggregateByKey(new StatCounter(), StatCounter::merge, StatCounter::merge)
                // reduceByKey sums the counter, the passengers and the fare_amount in order to compute the mean in the next method in the chain
                // (total_taxi_rides, (DOLocationID, fare_amount_mean, fare_amount_stdev))
                .mapToPair(tup -> new Tuple2<>(tup._2.count(), new Tuple3<>(tup._1, tup._2.mean(), tup._2.stdev())))
                // Order descending by number of taxi_rides to each location
                // Keep only top-5 locations
                .takeOrdered(RANKING_SIZE, (tup1, tup2) -> tup2._1.compareTo(tup1._1));


        System.out.println("=========== Ranking =============");
        for (int i = 0, collectSize = take.size(); i < collectSize; i++) {
            Tuple2<Long, Tuple3<Long, Double, Double>> res = take.get(i);
            System.out.printf("%d) taxi_rides = %d, locationID = %d, mean_fare_amount = %g $, stdev_fare_amount = %s $\n", i + 1, res._1, res._2._1(), res._2._2(), new DecimalFormat("#.##").format(res._2._3()));
        }
    }
}
