package com.sparkling_taxi;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;
import scala.Tuple4;

import java.sql.Timestamp;
import java.util.*;

// docker cp backup/Query2.parquet namenode:/home/Query2.parquet
// hdfs dfs -put Query2.parquet /home/dataset-batch/Query2.parquet
public class Query2 {

    public static final String FILE_Q2 = "hdfs://namenode:9000/home/dataset-batch/Query2.parquet";
    public static final int PICKUP_COL = 0;
    public static final int DROPOFF_COL = 1;
    public static final int TIP_AMOUNT_COL = 2;
    public static final int PAYMENT_TYPE_COL = 3;

    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .master("spark://spark:7077")
                .appName("Query2")
                .getOrCreate();
        spark.sparkContext().setLogLevel("WARN");

        JavaPairRDD javaPairRDD = Performance.measure("Query completa", () -> query2PerHour(spark, FILE_Q2));
    }

    /**
     * Hourly distribution of
     * - mean number od trips each hour
     * - mean AND standard deviation of tips each hour
     * - most popular payment method each hour (MAX number of occurrence)
     * TODO: dovremmo raggruppare per ora, quindi avere 24 chiavi diverse.
     *
     * @param spark
     * @param file
     */
    private static JavaPairRDD query2PerHour(SparkSession spark, String file) {
        List<Tuple2<Tuple2<BitSet, Long>, Integer>> rdd = spark.read().parquet(file)
                .toJavaRDD()
                .filter(row -> {
                    boolean x = true;
                    for (int i = 0; i < row.size(); i++) {
                        x = x && !row.isNullAt(i);
                    }
                    return x;
                }).mapToPair(row -> {
                    Timestamp timestamp = row.getTimestamp(PICKUP_COL);
                    Timestamp timestamp2 = row.getTimestamp(DROPOFF_COL);
                    int hourStart = Utils.toLocalDateTime(timestamp).getHour();
                    int hourEnd = Utils.toLocalDateTime(timestamp2).getHour();
                    BitSet hours = hourSlots(hourStart, hourEnd);
//                    for (int i = 0, integersLength = integers.length; i < integersLength; i++) {
//                        if (integers[i] == 1) {
//                            Tuple5<Integer, Integer, Double, Double, Long> tup5 = new Tuple5<>(
//                                    i, // hour
//                                    1, // counts the trips each hour
//                                    tup2._2()._1(),
//                                    tup2._2()._1() * tup2._2()._1(),
//                                    tup2._2()._2());
//                            tups.add(tup5);
//                        }
//                    }
                    return new Tuple2<>(hours, new Tuple2<>(row.getDouble(TIP_AMOUNT_COL), row.getLong(PAYMENT_TYPE_COL)));
                    // return new Tuple2<>(new Tuple2<>(hourStart, hourEnd), new Tuple2<>(row.getDouble(TIP_AMOUNT_COL), row.getLong(PAYMENT_TYPE_COL)));
                    // NEW tuple2: (hourSLOTS, (tip_amount, payment_tipe))
                    // OLD tuple2 : (hour, (1, tip_amount, square_tip_amount, payment_tipe))
                })
                .mapToPair(tup2 -> new Tuple2<>(new Tuple2<>(tup2._1, tup2._2._2()), 1))
                .take(5);
                // .reduceByKey((a, b) -> a + b)

//                .reduceByKey((v1, v2) -> (v1 > v2) ? v1 : v2)
//                .take(5);

        System.out.println("=============== Im here =============== ");

        rdd.forEach(x -> System.out.println(x));

        // System.out.println("=================== flatMap done (" + rdd.count() + ") ===================");

        // tuple2: (hour, (hour_taxi_trips, hour_tip_amount, hour_square_tip_amount))
//        List<Tuple2<Integer, Tuple3<Integer, Double, Double>>> collect1 = rdd.mapValues(x -> new Tuple3<>(x._1(), x._2(), x._3()))
//                .reduceByKey((x, y) -> new Tuple3<>(x._1() + y._1(), x._2() + y._2(), x._3() + y._3()))
//                .mapToPair(x -> new Tuple2<>(x._1(), new Tuple3<>(
//                        x._2._1(), // hour_trips
//                        x._2._2() / x._2._1(), // mean_hour_tip_amount
//                        Utils.stddev(x._2._1(), x._2()._2(), x._2()._3()))))
//                .collect();
//
//        collect1.forEach(System.out::println);
//        // stddev_hour_tip_amount
//        // (hour, (1, tip_amount, square_tip_amount, payment_type)) -> ((hour, payment_type), 1)
//        System.out.println("=================== statistics done ===================");
//
//        List<Tuple2<Integer, Long>> collect = rdd.mapToPair(tup2_4 -> new Tuple2<>(new Tuple2<>(tup2_4._1, tup2_4._2()._4()), 1))
//                .reduceByKey(Integer::sum) // 6*24 elementi (12:00, 3, 52),
//                // ((hour, payment_type), sum_payment_type) -> ((hour, payment_type), max_payment_type))
//                .reduceByKey((v1, v2) -> {
//                    if (v1 > v2) return v1;
//                    else return v2;
//                }).map(Tuple2::_1).collect();
//
//        System.out.println("=================== payment done ===================");
//
//        collect.forEach(System.out::println);

        return null;
    }

    public static BitSet hourSlots(int hourStart, int hourEnd) {
        // hour zone are: 0,1,2,3,...,23 where 0 = 00:00-00:59, 23=23:00-23:59
        BitSet b = new BitSet(24);
        if (hourStart == hourEnd) {
            b.set(hourStart);
            return b;
        }
        int counter = hourStart;
        while (counter != hourEnd + 1) {
            b.set(counter);
            counter = (counter + 1) % 24;
        }
        return b;
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
