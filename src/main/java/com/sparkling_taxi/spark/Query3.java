package com.sparkling_taxi.spark;

import com.sparkling_taxi.utils.Performance;
import com.sparkling_taxi.utils.Utils;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.util.StatCounter;
import scala.Tuple2;
import scala.Tuple3;
import scala.Tuple4;

import java.io.Serializable;
import java.text.DecimalFormat;
import java.util.Comparator;
import java.util.List;

// docker cp backup/Query3.parquet namenode:/home/Query3.parquet
// hdfs dfs -put Query3.parquet /home/dataset-batch/Query3.parquet
public class Query3 {
    /***
     * Identify the top-5 most popular DOLocationIDs, indicating for each one:
     * - the average number of passengers,
     * - the mean and standard deviation of fare_amount
     * @param args
     */
    public static final String FILE_Q3 = "hdfs://namenode:9000/home/dataset-batch/Query3.parquet";
    public static final int DO_LOC_COL = 3;
    public static final int PASSENGER_COUNT_COL = 2;
    public static final int FARE_AMOUNT_COL = 5;
    private static final int RANKING_SIZE = 5;

    public static void main(String[] args) {
        try (SparkSession spark = SparkSession
                .builder()
                .master("spark://spark:7077")
                .appName("Query3")
                .getOrCreate()
        ) {
            // spark.sparkContext().setLogLevel("WARN");
            // Performance.measure("Query3 - file4", () -> mostPopularDestination(spark, FILE_Q3));
            // Performance.measure("Query3 - file4", () -> mostPopularDestinationWithStdDev(spark, FILE_Q3));
            Performance.measure("Query3 - file4", () -> mostPopularDestinationWithTrueStdDev(spark, FILE_Q3));
        }
    }

    private static void mostPopularDestination(SparkSession spark, String file) {
        // (DOLocationID, 1, passeggeri, fare_amount)
        // reduceByKey() -> (DOLocation, numero_corse, somma_passeggeri, somma_fare_amount)
        // reduceByKey((x, y) -> (x >= y)) -> ranking di (DOLocation, numero_corse)
        // map(tup -> new (tup._1, tup._2, tup._3 / tup._2, tup._4 / tup._2)

        // Every element in the PairRdd contains: (DOLocationID, (1, passengers, fare_amount))
        // the "1" is used to count the occurrence of the location ???
        List<Tuple2<Integer, Tuple3<Long, Double, Double>>> collect = spark.read().parquet(file)
                .toJavaRDD()
                // excluding the rows with at least one null value in the necessary columns
                .filter(row -> !(row.isNullAt(DO_LOC_COL) || row.isNullAt(PASSENGER_COUNT_COL) || row.isNullAt(FARE_AMOUNT_COL)))
                // mapping the entire row to a new tuple with only the useful fields (and a 1 to count)
                .mapToPair(row -> new Tuple2<>(row.getLong(DO_LOC_COL), new Tuple3<>(1, row.getDouble(PASSENGER_COUNT_COL), row.getDouble(FARE_AMOUNT_COL))))
                // reduceByKey sums the counter, the passengers and the fare_amount in order to compute the mean in the next method in the chain
                // (DOLocationID, (total_taxi_rides, passengers_sum, fare_amount_sum))
                .reduceByKey((x, y) -> new Tuple3<>(x._1() + y._1(), x._2() + y._2(), x._3() + y._3()))
                // (total_taxi_rides, (DOLocationID, passenger_mean, fare_amount_mean))
                .mapToPair(tup -> new Tuple2<>(tup._2._1(), new Tuple3<>(tup._1, tup._2._2() / tup._2._1(), tup._2._3() / tup._2._1())))
                // Order descending by number of taxi_rides to each location
                .sortByKey(false)
                // Keep only top-5 locations
                .take(RANKING_SIZE);


        System.out.println("=========== Ranking =============");
        for (int i = 0, collectSize = collect.size(); i < collectSize; i++) {
            Tuple2<Integer, Tuple3<Long, Double, Double>> res = collect.get(i);
            System.out.printf("%d) taxi_rides = %d, locationID = %d, mean_passengers = %g, mean_fare_amount = %s $\n", i + 1, res._1, res._2._1(), res._2._2(), new DecimalFormat("#.##").format(res._2._3()));
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

    private static void mostPopularDestinationWithTrueStdDev(SparkSession spark, String file) {
        // (DOLocationID, 1, passeggeri, fare_amount)
        // reduceByKey() -> (DOLocation, numero_corse, somma_passeggeri, somma_fare_amount)
        // reduceByKey((x, y) -> (x >= y)) -> ranking di (DOLocation, numero_corse)
        // map(tup -> new (tup._1, tup._2, tup._3 / tup._2, tup._4 / tup._2)

        // Every element in the PairRdd contains: (DOLocationID, (1, passengers, fare_amount))
        // the "1" is used to count the occurrence of the location ???
        // TODO: vedere se bisogna rimuovere le colonne Dropoff e Pickup (su NiFi)
        List<Tuple2<Integer, Tuple4<Long, Double, Double, Double>>> take = spark.read().parquet(file)
                .toJavaRDD()
                // excluding the rows with at least one null value in the necessary columns
                .filter(row -> !(row.isNullAt(DO_LOC_COL) || row.isNullAt(PASSENGER_COUNT_COL) || row.isNullAt(FARE_AMOUNT_COL)))
                // mapping the entire row to a new tuple with only the useful fields (and a 1 to count)
                .mapToPair(row -> new Tuple2<>(row.getLong(DO_LOC_COL), new Tuple4<>(1, row.getDouble(PASSENGER_COUNT_COL), row.getDouble(FARE_AMOUNT_COL), row.getDouble(FARE_AMOUNT_COL) * row.getDouble(FARE_AMOUNT_COL))))
                // reduceByKey sums the counter, the passengers and the fare_amount in order to compute the mean in the next method in the chain
                // (DOLocationID, (total_taxi_rides, passengers_sum, fare_amount_sum, fare_amount_square_sum))
                .reduceByKey((x, y) -> new Tuple4<>(x._1() + y._1(), x._2() + y._2(), x._3() + y._3(), x._4() + y._4()))
                // (total_taxi_rides, (DOLocationID, passenger_mean, fare_amount_mean, fare_amount_stddev))
                // (sumOfSquares/count) - (sum/count)^2 :: Var(X)=E[X^2]-E^2[X] -> sigma(X)=sqrt(Var(X)/N)
                .mapToPair(tup -> new Tuple2<>(tup._2._1(), // count
                        new Tuple4<>(tup._1, // location
                                tup._2._2() / tup._2._1(), // mean_passengers
                                tup._2._3() / tup._2._1(), // mean_fare_amount
                                Utils.stddev(tup._2()._1(), tup._2._3(), tup._2._4())))) // stdev_fare_amount
                // Order descending by number of taxi_rides to each location
                // Keep only top-5 locations
                .takeOrdered(RANKING_SIZE, MyTupleComparator.INSTANCE);


        System.out.println("=========== Ranking =============");
        for (int i = 0, collectSize = take.size(); i < collectSize; i++) {
            Tuple2<Integer, Tuple4<Long, Double, Double, Double>> res = take.get(i);
            System.out.printf("%d) taxi_rides = %d, locationID = %d, mean_passengers = %g, mean_fare_amount = %s $ stdev_fare_amount %s $\n",
                    i + 1, // rank
                    res._1, //taxi_rides
                    res._2._1(), // location
                    res._2._2(), // mean_passengers
                    new DecimalFormat("#.##").format(res._2._3()), // mean_fare_amount
                    new DecimalFormat("#.##").format(res._2._4()) // stdev_fare_amount
            );
        }
    }

    private static class MyTupleComparator implements Comparator<Tuple2<Integer, Tuple4<Long, Double, Double, Double>>>, Serializable {
        final static MyTupleComparator INSTANCE = new MyTupleComparator();

        @Override
        public int compare(Tuple2<Integer, Tuple4<Long, Double, Double, Double>> o1, Tuple2<Integer, Tuple4<Long, Double, Double, Double>> o2) {
            return -o1._1.compareTo(o2._1); // descending
        }
    }
//    private static class FavouriteDestination extends Tuple2<Long, RideStatistics> {
//
//        public FavouriteDestination(Long destination, RideStatistics stats) {
//            super(destination, stats);
//        }
//    }
//
//    private static class RideStatistics {
//        private int taxi_rides;
//        private double passengers_mean;
//        private double fare_amount_mean;
//        private double fare_amount_stddev;
//
//        public RideStatistics(Integer _1, Double _2, Double _3, Double _4) {
//            taxi_rides = _1;
//            passengers_mean = _2;
//            fare_amount_mean = _3;
//            fare_amount_stddev = _4;
//        }
//    }
}
