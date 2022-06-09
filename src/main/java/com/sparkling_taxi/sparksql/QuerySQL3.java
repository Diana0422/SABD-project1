package com.sparkling_taxi.sparksql;
import com.sparkling_taxi.bean.query1.CSVQuery1;
import com.sparkling_taxi.bean.query3.CSVQuery3;
import com.sparkling_taxi.bean.query3.Zone;
import com.sparkling_taxi.spark.Query;
import com.sparkling_taxi.utils.Performance;
import com.sparkling_taxi.utils.Utils;
import lombok.var;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.*;
import redis.clients.jedis.Jedis;
import scala.Tuple6;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.sparkling_taxi.spark.Query3.RANKING_SIZE;
import static com.sparkling_taxi.utils.Const.*;
import static org.apache.spark.sql.functions.*;

public class QuerySQL3 extends Query<CSVQuery3> {
    public QuerySQL3(){
        super();
    }
    public static void main(String[] args) {
        QuerySQL3 q3 = new QuerySQL3();
        q3.preProcessing(); // NiFi pre-processing
        List<CSVQuery3> csvQuery3Dataset = q3.processing();
        q3.postProcessing(csvQuery3Dataset); // Save output to Redis
        q3.closeSession();
    }

    public void preProcessing() {
        Utils.doPreProcessing(FILE_Q3, PRE_PROCESSING_TEMPLATE_Q3);
    }

    //TODO sistemare output finale
    //TODO fare scrittura su redis
    public List<CSVQuery3> processing() {
        Dataset<Row> parquet = spark.read().parquet(FILE_Q3);
        Dataset<Row> rowDataset = parquet.toDF("dropoff", "passengers", "location", "fare_amount");
        Dataset<Row> convertDay = rowDataset.withColumn("day", to_date(col("dropoff"), "yyyy-MM-dd"))
                .select("day", "passengers", "location", "fare_amount");
        convertDay.show();
        convertDay.createOrReplaceTempView("query3");

//
//        String sqlQuery = "SELECT location ,count(*) as trips, avg(passengers), stddev(fare_amount), avg(fare_amount)\n" +
//                          "FROM query3\n" +
//                          "GROUP BY location\n" +
//                          "ORDER BY trips DESC\n" +
//                          "LIMIT 5";

        String sql = "SELECT day, location, count(*) AS occurrence, avg(passengers) as avg_passengers, " +
                "avg(fare_amount) as avg_fare_amount, stddev(fare_amount) as stddev_fare_amount" +
                " FROM query3 GROUP BY day, location";
        Dataset<Row> sql1 = spark.sql(sql);
        sql1.show();
        sql1.createOrReplaceTempView("sql1");

        String calcTopRanking = "SELECT day, location, occurrence, avg_passengers, avg_fare_amount, stddev_fare_amount," +
                "row_number() over (PARTITION BY day ORDER BY occurrence DESC) as rank" +
                " FROM sql1";
        Dataset<Row> sql2 = spark.sql(calcTopRanking);
        sql2.show();
        sql2.createOrReplaceTempView("sql2");

        String calcTop5 = "SELECT * FROM sql2 WHERE rank <= 5";
        Dataset<Row> result = spark.sql(calcTop5);

//        Map<String, String> zones = spark.read()
//                .option("header", false)
//                .csv(ZONES_CSV)
//                .toDF("LocationID", "Borough", "Zone", "service_zone")
//                .drop("service_zone")
//                .collectAsList()
//                .stream()
//                .map(row -> new Zone(row.getString(0), row.getString(1), row.getString(2)))
//                .collect(Collectors.toMap(Zone::getId, Zone::zoneString));
//
//        Dataset<Row> rowDataset1 = result.withColumn("location", col("location").cast("string"));
//        res = rowDataset1.map((MapFunction<Row, Row>)  row -> {
//            String regex = row.getString(1);
//            regexp_replace(col("location"), regex, zones.get(regex));
//        });
//
//
//        DataFrameWriter<Row> finalResult = spark.createDataFrame(result, CSVQuery3.class)
//                .select("day",
//                        "location1", "location2", "location3", "location4", "location5",
//                        "trips1", "trips2", "trips3", "trips4", "trips5",
//                        "meanPassengers1", "meanPassengers2", "meanPassengers3", "meanPassengers4", "meanPassengers5",
//                        "meanFareAmount1", "meanFareAmount2", "meanFareAmount3", "meanFareAmount4", "meanFareAmount5",
//                        "stDevFareAmount1", "stDevFareAmount2", "stDevFareAmount3", "stDevFareAmount4", "stDevFareAmount5")
//                .write()
//                .mode("overwrite")
//                .option("header", true)
//                .option("delimiter", ";");
//
//        finalResult.csv(OUT_HDFS_URL_Q3);
//        this.copyAndRenameOutput(OUT_HDFS_URL_Q3, RESULT_DIR3);
//
//        // REDIS
//        try (Jedis jedis = new Jedis("redis://redis:6379")) {
//            for (CSVQuery3 q : query3CsvList) {
//                HashMap<String, String> m = new HashMap<>();
//                m.put("day", q.getDay());
//                m.put("location1", q.getLocation1());
//                m.put("location2", q.getLocation2());
//                m.put("location3", q.getLocation3());
//                m.put("location4", q.getLocation4());
//                m.put("location5", q.getLocation5());
//                m.put("trips1", q.getTrips1());
//                m.put("trips2", q.getTrips2());
//                m.put("trips3", q.getTrips3());
//                m.put("trips4", q.getTrips4());
//                m.put("trips5", q.getTrips5());
//                m.put("meanPassengers1", q.getMeanPassengers1());
//                m.put("meanPassengers2", q.getMeanPassengers2());
//                m.put("meanPassengers3", q.getMeanPassengers3());
//                m.put("meanPassengers4", q.getMeanPassengers4());
//                m.put("meanPassengers5", q.getMeanPassengers5());
//                m.put("meanFareAmount1", q.getMeanFareAmount1());
//                m.put("meanFareAmount2", q.getMeanFareAmount2());
//                m.put("meanFareAmount3", q.getMeanFareAmount3());
//                m.put("meanFareAmount4", q.getMeanFareAmount4());
//                m.put("meanFareAmount5", q.getMeanFareAmount5());
//                m.put("stdDevFareAmount1", q.getStDevFareAmount1());
//                m.put("stdDevFareAmount2", q.getStDevFareAmount2());
//                m.put("stdDevFareAmount3", q.getStDevFareAmount3());
//                m.put("stdDevFareAmount4", q.getStDevFareAmount4());
//                m.put("stdDevFareAmount5", q.getStDevFareAmount5());
//                jedis.hset(q.getDay(), m);
//            }
//        }
//
//
//        spark.close();
//        return result.as(Encoders.bean(CSVQuery3.class)).collectAsList();
        return new ArrayList<>();
    }

    public void closeSession() {

    }

    public void postProcessing(List<CSVQuery3> csvQuery3Dataset) {

    }
}
