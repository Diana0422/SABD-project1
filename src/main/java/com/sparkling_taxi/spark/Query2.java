package com.sparkling_taxi.spark;

import com.sparkling_taxi.bean.query2.*;
import com.sparkling_taxi.bean.query3.Query3Calc;
import com.sparkling_taxi.utils.Performance;
import com.sparkling_taxi.utils.Utils;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;
import scala.Tuple4;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static com.sparkling_taxi.utils.Const.*;
import static com.sparkling_taxi.utils.Utils.intRange;

// docker cp backup/Query2.parquet namenode:/home/Query2.parquet
// hdfs dfs -put Query2.parquet /home/dataset-batch/Query2.parquet
public class Query2 {
    // public static final String FILE_Q2 = "hdfs://namenode:9000/home/dataset-batch/LittleQuery2.parquet";
    // public static final String PARTIAL_OUTPUT = "hdfs://namenode:9000/home/dataset-batch/partOutputQ2";
    public static final int PICKUP_COL = 0;
    public static final int DROPOFF_COL = 1;
    public static final int TIP_AMOUNT_COL = 2;
    public static final int PAYMENT_TYPE_COL = 3;

    public static void main(String[] args) {
        Query2 q = new Query2();
        q.preProcessing();
        q.runQuery();
        q.postProcessing();
    }

    public void preProcessing() {
        Utils.doPreProcessing(FILE_Q2, PRE_PROCESSING_TEMPLATE_Q2);
    }

    public void runQuery() {
        SparkSession spark = SparkSession
                .builder()
                .master("spark://spark:7077")
                .appName("Query2")
                .getOrCreate();
        spark.sparkContext().setLogLevel("WARN");

//        Performance.measure("Query completa", () -> query2PerHourWithGroupBy(spark, FILE_Q2));
        Performance.measure("Query completa", () -> query2V2(spark, FILE_Q2));
    }

    public void postProcessing() {
        //TODO grafana redis interaction
    }

    /**
     * Hourly distribution of
     * - mean number of trips each hour
     * - mean of tips each hour
     * - standard deviation of tips each hour
     * - most popular payment method each hour (MAX number of occurrence)
     */
    public static JavaPairRDD query2PerHourWithGroupBy(SparkSession spark, String file) {
        JavaRDD<Query2Bean> rdd = spark.read().parquet(file)
                .as(Encoders.bean(Query2Bean.class))
                .toJavaRDD();
        // (hour, Query2Calc)
        JavaPairRDD<String, Query2Calc> hourCalc = rdd.mapToPair(bean -> new Tuple2<>(Utils.getHourDay(bean.getTpep_pickup_datetime()), new Query2Calc(1, bean))).cache();

        JavaPairRDD<String, Query2Calc> reduced = hourCalc.reduceByKey(Query2Calc::sumWith).cache();

        JavaPairRDD<String, Query2Result> result = reduced.mapValues(Query2Result::new);

        result.collect().forEach(System.out::println);
        return null;
    }

    public static JavaPairRDD query2V2(SparkSession spark, String file) {
        JavaRDD<Query2Bean> rdd = spark.read().parquet(file)
                .as(Encoders.bean(Query2Bean.class))
                .toJavaRDD()
                .cache();
        // partial 1: calculate total trips + total tip + distribution
        JavaPairRDD<String, Query2CalcV2> hourCalc = rdd.mapToPair(bean -> new Tuple2<>(Utils.getHourDay(bean.getTpep_pickup_datetime()), new Query2CalcV2(1, bean)));
        JavaPairRDD<String, Query2CalcV2> reduced1 = hourCalc.reduceByKey(Query2CalcV2::sumWith);
        JavaPairRDD<String, Query2ResultV2> partial1 = reduced1.mapValues(Query2ResultV2::new).cache();


        // partial 2: calculate number of trips for each PULocation
        JavaPairRDD<Tuple2<String, Long>, Integer> locationMapping = rdd.mapToPair(bean -> new Tuple2<>(new Tuple2<>(Utils.getHourDay(bean.getTpep_pickup_datetime()), bean.getPayment_type()), 1));
        JavaPairRDD<Tuple2<String, Long>, Integer> reduced = locationMapping.reduceByKey(Integer::sum); // ((dayHour, paymentType), 1) -> ((dayHour, paymentType), occorrenzaxtipo))

        JavaPairRDD<String, Tuple2<Long, Integer>> result = reduced.mapToPair(t -> new Tuple2<>(t._1._1, new Tuple2<>(t._1._2, t._2))) // ((dayHour, paymentType), occorrenzaxtipo)) -> (dayHour, (paymentType, occorrenza))
                .reduceByKey((t1, t2) -> { // (dayHour, (paymentType1, occorrenza)) , (dayHour, (paymentType2, occorrenza))
                    if (t1._2 >= t2._2) return t1;
                    else return t2;
                }).cache(); // (dayHour, (payment, occurrence))

        JavaPairRDD<String, Tuple2<Tuple2<Long, Integer>, Query2ResultV2>> join = result.join(partial1).cache();
        join.collect();
        return null;
    }

    public static List<Integer> hourSlotsList(int hourStart, int hourEnd) {
        // hour zone are: 0,1,2,3,...,23 where 0 = 00:00-00:59, 23=23:00-23:59
        if (hourStart == hourEnd) {
            return Collections.singletonList(hourStart);
        } else if (hourStart < hourEnd) {
            return intRange(hourStart, hourEnd + 1);
        } else {
            List<Integer> integers = intRange(hourStart, 24);
            integers.addAll(intRange(0, hourEnd + 1));
            return new ArrayList<>(new HashSet<>(integers));
        }
    }

    private static class PopularPaymentType {
        private final Map<PaymentType, Integer> typeOccurrences;

        private PopularPaymentType() {
            this.typeOccurrences = new HashMap<>();
            for (PaymentType value : PaymentType.values()) {
                typeOccurrences.put(value, 0);
            }
        }

        public void incrementPaymentType(Integer i1, Integer i2) {
            PaymentType paymentType1 = PaymentType.valueOf(i1.toString());
            PaymentType paymentType2 = PaymentType.valueOf(i2.toString());

            Integer t1 = typeOccurrences.get(paymentType1);
            typeOccurrences.put(paymentType1, t1 + 1);
            Integer t2 = typeOccurrences.get(paymentType2);
            typeOccurrences.put(paymentType2, t2 + 1);
        }

        private PaymentType getMostPopular() {
            PaymentType argmax = PaymentType.CreditCard;
            int max = 0;
            for (PaymentType p : typeOccurrences.keySet()) {
                int occ = typeOccurrences.get(p);
                if (occ > max) {
                    argmax = p;
                    max = occ;
                }
            }
            return argmax;
        }

        public PopularPaymentType merge(Tuple4<Integer, Double, Double, Integer> p1) {
            return null;
        }
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
