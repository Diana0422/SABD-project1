package com.sparkling_taxi.spark;

import com.sparkling_taxi.bean.QueryResult;
import com.sparkling_taxi.bean.query2.*;
import com.sparkling_taxi.evaluation.Performance;
import com.sparkling_taxi.utils.Utils;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.*;
import redis.clients.jedis.Jedis;
import scala.Tuple2;

import java.util.HashMap;
import java.util.List;

import static com.sparkling_taxi.utils.Const.*;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.split;

// docker cp backup/Query2.parquet namenode:/home/Query2.parquet
// hdfs dfs -put Query2.parquet /home/dataset-batch/Query2.parquet
public class Query2 extends Query<Query2Result> {

    public Query2(SparkSession s) {
        super(s);
    }

    public Query2(boolean b, SparkSession s) {
        super(b, s);
    }

    public static void main(String[] args) {
        Query2 q = new Query2();
        q.preProcessing();
        List<Query2Result> result = Performance.measure("Query completa", q::processing);
        q.postProcessing(result);
        q.closeSession();
    }

    public Query2() {
        super();
    }

    public void preProcessing() {
        Utils.doPreProcessing(FILE_Q2, PRE_PROCESSING_TEMPLATE_Q2, forcePreprocessing);
    }

    public List<Query2Result> processing() {
        System.out.println("======================= Running " + this.getClass().getSimpleName() + " =======================");
        return query2PerHourWithGroupBy(spark, FILE_Q2);
    }

    public void postProcessing(List<Query2Result> result) {
        List<CSVQuery2> csvListResult = jc.parallelize(result)
                // sometimes spark produces two partitions (two output files) but the output has only 3 lines,
                // so we force it to use only 1 partition
                .map(CSVQuery2::new)
                .collect();
        storeToCSVOnHDFS(csvListResult, this);
        storeQuery2ToRedis(csvListResult);
    }

    public static <T extends QueryResult> void storeToCSVOnHDFS(List<CSVQuery2> query2CsvList, Query<T> q) {
        // Dataframe is NOT statically typed, but uses less memory (GC) than dataset
        Dataset<Row> rowDataset = q.spark.createDataFrame(query2CsvList, CSVQuery2.class);

        Column locationDistribution = split(col("locationDistribution"), "-");

        for (int i = 0; i < NUM_LOCATIONS; i++) {
            rowDataset = rowDataset.withColumn("perc_PU" + (i + 1), locationDistribution.getItem(i));
        }
        DataFrameWriter<Row> finalResult = rowDataset
                .drop("locationDistribution")
                .coalesce(1)
                .select("hour", "avgTip", "stdDevTip", "popPayment", "perc_PU1", "perc_PU2", "perc_PU3", "perc_PU4", "perc_PU5", "perc_PU6",
                        "perc_PU7", "perc_PU8", "perc_PU9", "perc_PU10", "perc_PU11", "perc_PU12", "perc_PU13", "perc_PU14", "perc_PU15",
                        "perc_PU16", "perc_PU17", "perc_PU18", "perc_PU19", "perc_PU20", "perc_PU21", "perc_PU22", "perc_PU23", "perc_PU24",
                        "perc_PU25", "perc_PU26", "perc_PU27", "perc_PU28", "perc_PU29", "perc_PU30", "perc_PU31", "perc_PU32", "perc_PU33",
                        "perc_PU34", "perc_PU35", "perc_PU36", "perc_PU37", "perc_PU38", "perc_PU39", "perc_PU40", "perc_PU41", "perc_PU42",
                        "perc_PU43", "perc_PU44", "perc_PU45", "perc_PU46", "perc_PU47", "perc_PU48", "perc_PU49", "perc_PU50", "perc_PU51",
                        "perc_PU52", "perc_PU53", "perc_PU54", "perc_PU55", "perc_PU56", "perc_PU57", "perc_PU58", "perc_PU59", "perc_PU60",
                        "perc_PU61", "perc_PU62", "perc_PU63", "perc_PU64", "perc_PU65", "perc_PU66", "perc_PU67", "perc_PU68", "perc_PU69",
                        "perc_PU70", "perc_PU71", "perc_PU72", "perc_PU73", "perc_PU74", "perc_PU75", "perc_PU76", "perc_PU77", "perc_PU78",
                        "perc_PU79", "perc_PU80", "perc_PU81", "perc_PU82", "perc_PU83", "perc_PU84", "perc_PU85", "perc_PU86", "perc_PU87",
                        "perc_PU88", "perc_PU89", "perc_PU90", "perc_PU91", "perc_PU92", "perc_PU93", "perc_PU94", "perc_PU95", "perc_PU96",
                        "perc_PU97", "perc_PU98", "perc_PU99", "perc_PU100", "perc_PU101", "perc_PU102", "perc_PU103", "perc_PU104",
                        "perc_PU105", "perc_PU106", "perc_PU107", "perc_PU108", "perc_PU109", "perc_PU110", "perc_PU111", "perc_PU112",
                        "perc_PU113", "perc_PU114", "perc_PU115", "perc_PU116", "perc_PU117", "perc_PU118", "perc_PU119", "perc_PU120",
                        "perc_PU121", "perc_PU122", "perc_PU123", "perc_PU124", "perc_PU125", "perc_PU126", "perc_PU127", "perc_PU128",
                        "perc_PU129", "perc_PU130", "perc_PU131", "perc_PU132", "perc_PU133", "perc_PU134", "perc_PU135", "perc_PU136",
                        "perc_PU137", "perc_PU138", "perc_PU139", "perc_PU140", "perc_PU141", "perc_PU142", "perc_PU143", "perc_PU144",
                        "perc_PU145", "perc_PU146", "perc_PU147", "perc_PU148", "perc_PU149", "perc_PU150", "perc_PU151", "perc_PU152",
                        "perc_PU153", "perc_PU154", "perc_PU155", "perc_PU156", "perc_PU157", "perc_PU158", "perc_PU159", "perc_PU160",
                        "perc_PU161", "perc_PU162", "perc_PU163", "perc_PU164", "perc_PU165", "perc_PU166", "perc_PU167", "perc_PU168",
                        "perc_PU169", "perc_PU170", "perc_PU171", "perc_PU172", "perc_PU173", "perc_PU174", "perc_PU175", "perc_PU176",
                        "perc_PU177", "perc_PU178", "perc_PU179", "perc_PU180", "perc_PU181", "perc_PU182", "perc_PU183", "perc_PU184",
                        "perc_PU185", "perc_PU186", "perc_PU187", "perc_PU188", "perc_PU189", "perc_PU190", "perc_PU191", "perc_PU192",
                        "perc_PU193", "perc_PU194", "perc_PU195", "perc_PU196", "perc_PU197", "perc_PU198", "perc_PU199", "perc_PU200",
                        "perc_PU201", "perc_PU202", "perc_PU203", "perc_PU204", "perc_PU205", "perc_PU206", "perc_PU207", "perc_PU208",
                        "perc_PU209", "perc_PU210", "perc_PU211", "perc_PU212", "perc_PU213", "perc_PU214", "perc_PU215", "perc_PU216",
                        "perc_PU217", "perc_PU218", "perc_PU219", "perc_PU220", "perc_PU221", "perc_PU222", "perc_PU223", "perc_PU224",
                        "perc_PU225", "perc_PU226", "perc_PU227", "perc_PU228", "perc_PU229", "perc_PU230", "perc_PU231", "perc_PU232",
                        "perc_PU233", "perc_PU234", "perc_PU235", "perc_PU236", "perc_PU237", "perc_PU238", "perc_PU239", "perc_PU240",
                        "perc_PU241", "perc_PU242", "perc_PU243", "perc_PU244", "perc_PU245", "perc_PU246", "perc_PU247", "perc_PU248",
                        "perc_PU249", "perc_PU250", "perc_PU251", "perc_PU252", "perc_PU253", "perc_PU254", "perc_PU255", "perc_PU256",
                        "perc_PU257", "perc_PU258", "perc_PU259", "perc_PU260", "perc_PU261", "perc_PU262", "perc_PU263", "perc_PU264", "perc_PU265")
                .sort("hour")
                .write()
                .mode("overwrite")
                .option("header", true)
                .option("delimiter", ";");
        finalResult.csv(OUT_HDFS_URL_Q2);
        System.out.println("================== written csv to HDFS =================");

        q.copyAndRenameOutput(OUT_HDFS_URL_Q2, RESULT_DIR2);

        System.out.println("================== copied csv to local FS =================");

    }


    public static void storeQuery2ToRedis(List<CSVQuery2> csvListResult) {
        // REDIS
        try (Jedis jedis = new Jedis("redis://redis:6379")) {
            for (CSVQuery2 t : csvListResult) {
                String[] split = t.getLocationDistribution().split("-");
                HashMap<String, String> m = new HashMap<>();
                m.put("Hour", t.getHour());
                m.put("AvgTip", String.valueOf(t.getAvgTip()));
                m.put("StdDevTip", String.valueOf(t.getStdDevTip()));
                m.put("MostPopularPayment", String.valueOf(t.getPopPayment()));
                for (int i = 0; i < NUM_LOCATIONS; i++) {
                    String s = split[i];
                    if (!s.equals("0")) {
                        m.put("Loc" + (i + 1), s);
                    }
                }
                jedis.hset(t.getHour(), m);
            }
        }

        System.out.println("================= Stored on REDIS =================");
    }

    /**
     * Hourly distribution of
     * - percentage of trips each hour each start location
     * - mean of tips each hour
     * - standard deviation of tips each hour
     * - most popular payment method each hour (MAX number of occurrence)
     */
    public static List<Query2Result> query2PerHourWithGroupBy(SparkSession spark, String file) {
        return spark.read().parquet(file)
                .as(Encoders.bean(Query2Bean.class))
                .toJavaRDD()
                // (hour, Query2Calc(trip_count=1, tip_amount, square_tip_amount, payment_type_distribution, PULocation_trips_distribution))
                // Payment_type_distribution is a Map with value 1 for the payment type of the trip and 0 for the other 5 payment types
                // PULocation_trips_distribution is a Map with value 1 for the departure location of the trip and 0 for the other 264 locations
                .mapToPair(bean -> new Tuple2<>(Utils.getHourDay(bean.getTpep_pickup_datetime()), new Query2Calc(1, bean)))
                // sums all previous data and the result is:
                // (hour, Query2Calc(total_trip_count, sum_of_tips, sum_of_square_tips, final_payment_type_distribution, final_PULocation_trips_distribution))
                // where final_payment_type_distribution is a Map with the total number of trips paid with each payment type
                // and final_PULocation_trips_distribution is a Map with the total number of trips from each location
                .reduceByKey(Query2Calc::sumWith)
                // computes means, standard deviations, max payment types and location distribution of trips for each hour
                // (hour, Query2Result(avgTip, stdDevTip, mostPopularPaymentType, distribution_of_trips_for_265_locations)
                // where distribution_of_trips_for_265_locations is a Map with the percentage of trips from each location
                .map(Query2Result::new)
                .collect();
    }

    private enum PaymentType {
        CreditCard(1),
        Cash(2),
        NoCharge(3),
        Dispute(4),
        Unknown(5),
        VoidedTrip(6);
        private final int num;

        PaymentType(int num) {
            this.num = num;
        }

        public int getNum() {
            return num;
        }
    }
}
