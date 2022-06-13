package com.sparkling_taxi.sparksql;

import com.sparkling_taxi.bean.query3.CSVQuery3;
import com.sparkling_taxi.bean.query3.Zone;
import com.sparkling_taxi.spark.Query;
import com.sparkling_taxi.spark.Query3;
import com.sparkling_taxi.evaluation.Performance;
import com.sparkling_taxi.utils.Utils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.sparkling_taxi.utils.Const.*;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.to_date;

public class QuerySQL3 extends Query<CSVQuery3> {
    public QuerySQL3() {
        super();
    }

    public QuerySQL3(SparkSession s) {
        super(s);
    }

    public QuerySQL3(boolean b, SparkSession s) {
        super(b, s);
    }

    public static void main(String[] args) {
        QuerySQL3 q3 = new QuerySQL3();
        try {
            q3.preProcessing(); // NiFi pre-processing
            List<CSVQuery3> csvQuery3Dataset = Performance.measure(q3::processing);
            q3.postProcessing(csvQuery3Dataset); // Save output to Redis
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            q3.closeSession();
        }
    }

    /**
     * Does the preprocessing with using a fixed NiFi template, if the input file for processing is not already present.
     * If the field forcePreprocessing is true, the preprocessing is always done.
     * It also downloads the dataset, but only if they are not already downloaded on HDFS.
     */
    public void preProcessing() {
        Utils.doPreProcessing(FILE_Q3, PRE_PROCESSING_TEMPLATE_Q3, forcePreprocessing);
    }

    /**
     * Identify for EACH DAY the top-5 most popular DOLocationIDs, indicating for each one:
     * - the average number of passengers,
     * - the mean and standard deviation of fare_amount
     *
     * @return the CsvQuery3 result list
     */
    public List<CSVQuery3> processing() {
        System.out.println("======================= Running " + this.getClass().getSimpleName() + " =======================");
        Dataset<Row> parquet = spark.read().parquet(FILE_Q3);
        Dataset<Row> rowDataset = parquet.toDF("dropoff", "passengers", "location", "fare_amount");
        Dataset<Row> convertDay = rowDataset.withColumn("day", to_date(col("dropoff"), "yyyy-MM-dd"))
                .select("day", "passengers", "location", "fare_amount");
        convertDay.createOrReplaceTempView("query3");

        // groups by day and location the number of trips with the location as destination,
        // the average numbers of passengers and fare amount and also the fare amount stdev.
        // the only thing that remains to compute is the rank of each location
        String sql = "SELECT day, location, count(location) AS occurrence, avg(passengers) as avg_passengers, " +
                     "avg(fare_amount) as avg_fare_amount, stddev(fare_amount) as stddev_fare_amount" +
                     " FROM query3 GROUP BY day, location";
        Dataset<Row> sql1 = spark.sql(sql);
        sql1.createOrReplaceTempView("sql1");

        // row_number: assigns a row number to each occurrence in the day to each location, and orders them in descending order
        String calcTopRanking = "SELECT day, location, occurrence, avg_passengers, avg_fare_amount, stddev_fare_amount,\n" +
                "row_number() over (PARTITION BY day ORDER BY occurrence DESC) as rank \n" +
                "FROM sql1\n";
        Dataset<Row> sql2 = spark.sql(calcTopRanking);
        sql2.createOrReplaceTempView("sql2");

        // limits to get only the top 5 locations that day
        String calcTop5 = "SELECT * FROM sql2 WHERE rank <= 5\n";
        List<Row> query3 = spark.sql(calcTop5).collectAsList();

        // collects the taxi zones in a Map.
        Map<String, String> zones = spark.read()
                .option("header", false)
                .csv(ZONES_CSV)
                .toDF("LocationID", "Borough", "Zone", "service_zone")
                .drop("service_zone")
                .collectAsList()
                .stream()
                .map(row -> new Zone(row.getString(0), row.getString(1), row.getString(2)))
                .collect(Collectors.toMap(Zone::getId, Zone::zoneString));

        // gets five by five the most popular destinations and saves them in a single record (CSVQuery3)
        List<CSVQuery3> query3CsvList = new ArrayList<>();
        for (int i = 0; i < query3.size() / RANKING_SIZE; i++) {
            List<Row> five = getFive(query3, i);
            List<String> locations = new ArrayList<>();
            List<String> trips = new ArrayList<>();
            List<Double> meanPassengers = new ArrayList<>();
            List<Double> meanFareAmounts = new ArrayList<>();
            List<Double> stdDevFareAmounts = new ArrayList<>();
            for (Row row : five) {
                locations.add(zones.get(String.valueOf(row.getLong(1)))); // also maps the zones
                trips.add(String.valueOf(row.getLong(2)));
                meanPassengers.add(row.getDouble(3));
                meanFareAmounts.add(row.getDouble(4));
                stdDevFareAmounts.add(row.getDouble(5));
            }
            String day = five.get(0).getDate(0).toString();
            query3CsvList.add(new CSVQuery3(day, locations, trips, meanPassengers, meanFareAmounts, stdDevFareAmounts));
        }
        return query3CsvList;
    }

    /**
     * Saves the result on Redis and on HDFS in CSV format.
     * @param result a list of CSVQuery3
     */
    public void postProcessing(List<CSVQuery3> query3CsvList) {
        // saving the result on hdfs...
        Query3.storeToCSVOnHDFS(query3CsvList, this);
        System.out.println("================== written csv to HDFS =================");
        // ...and also on redis
        Query3.storeQuery3ToRedis(query3CsvList);
        System.out.println("================== copied csv to local FS =================");

    }

    /**
     * Get max five consecutive elements from a list, starting from a multiple of five.
     * @param list the list to get 5 elements from
     * @param offset the index of the multiple of five. For the first 5 elements, 0, for the second 5 elements, 1 and so on.
     * @return a list with max consecutive 5 elements from the input list.
     * @param <T> the generic type of the list.
     */
    public static <T> List<T> getFive(List<T> list, int offset) {
        int start = offset * 5;
        int size = list.size();
        if (start > size || start < 0) {
            return new ArrayList<>();
        }

        int end = Math.min(start + 5, size);
        List<T> five = new ArrayList<>();
        for (int i = start; i < end; i++) {
            five.add(list.get(i));
        }
        return five;
    }
}
