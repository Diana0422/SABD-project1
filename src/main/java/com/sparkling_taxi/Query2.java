package com.sparkling_taxi;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;
import scala.Tuple3;
import scala.Tuple4;

import java.sql.Timestamp;
import java.util.*;

// docker cp backup/Query2.parquet namenode:/home/Query2.parquet
// hdfs dfs -put Query2.parquet /home/dataset-batch/Query2.parquet
public class Query2 {

    public static final String FILE_Q2 = "hdfs://namenode:9000/home/dataset-batch/Query2.parquet";
    //public static final String FILE_Q2 = "hdfs://namenode:9000/home/dataset-batch/LittleQuery2.parquet";
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

        Performance.measure("Query completa", () -> query2PerHourWithGroupBy(spark, FILE_Q2));
    }

//    private static void query2PerHourV2(SparkSession spark, String fileQ2) {
//        // Dataset<Struct1Hour> dataset1 =
//
//        System.out.println(spark.read().parquet(fileQ2)
//                .filter((FilterFunction<Row>) row -> !(row.isNullAt(0) || row.isNullAt(1) || row.isNullAt(2) || row.isNullAt(3)))
//                .flatMap((FlatMapFunction<Row, Struct1Hour>) row -> {
//                    Timestamp timestamp = row.getTimestamp(PICKUP_COL);
//                    Timestamp timestamp2 = row.getTimestamp(DROPOFF_COL);
//                    int hourStart = Utils.toLocalDateTime(timestamp).getHour();
//                    int hourEnd = Utils.toLocalDateTime(timestamp2).getHour();
//                    List<Struct1Hour> ls = new ArrayList<>();
//                    int counter = hourStart;
//                    while (counter != hourEnd + 1) {
//                        ls.add(new Struct1Hour(counter, 1, row.getDouble(TIP_AMOUNT_COL), row.getDouble(TIP_AMOUNT_COL) * row.getDouble(TIP_AMOUNT_COL), row.getLong(PAYMENT_TYPE_COL)));
//                        counter = (counter + 1) % 24;
//                    }
//                    return ls.iterator();
//                    // for 3000 elements: 7s
//                }, Encoders.kryo(Struct1Hour.class))
//                .toJavaRDD()
//                .getNumPartitions());
//                .mapToPair(struct1Hour -> new Tuple2<>(struct1Hour.getHour(), new TipAndTrips(struct1Hour)))
//                .reduceByKey((v1, v2) -> new TipAndTrips(v1.getTripCount() + v2.getTripCount(),
//                        v1.getTipAmount() + v2.getTipAmount(),
//                        v1.getSquareTipAmount() + v2.getSquareTipAmount()))
//                .mapValues(tt -> new TipAndTrips(tt.getTripCount(),
//                        tt.getTipAmount() / tt.getTripCount(),
//                        Utils.stddev(tt.getTripCount(), tt.getTipAmount(), tt.getSquareTipAmount())))
//                .take(5)
//                .forEach(System.out::println);

//        dataset1.map((MapFunction<Struct1, Struct2>) s1 -> new Struct2(s1.getHourStart(), s1.getHourEnd(), s1.getTripCount(), s1.getTipAmount(), s1.getSquareTipAmount()), Encoders.kryo(Struct2.class))
//                .
//                .reduceGroups((ReduceFunction<Struct2>) new ReduceFunction<Struct2>() {
//                    @Override
//                    public Struct2 call(Struct2 v1, Struct2 v2) throws Exception {
//                        return new Struct2(v1);
//                    }
//                })
    // after reduceByKey: ((hourStart, hourEnd), (count, sum_tip_amount, sum_square_tip_amount))
    //.reduceByKey((a, b) -> new Tuple3<>(a._1() + b._1(), a._2() + a._2(), a._3() + b._3()));

//                .flatMap((FlatMapFunction<Tuple2<Tuple2<Integer,Integer>,Tuple4<Integer, Double, Double, Long>>, Tuple5<Integer, Integer, Double, Double, Long>>) tup -> {
//
//                    Timestamp timestamp = tup.getTimestamp(PICKUP_COL);
//                    Timestamp timestamp2 = row.getTimestamp(DROPOFF_COL);
//                    int hourStart = Utils.toLocalDateTime(timestamp).getHour();
//                    int hourEnd = Utils.toLocalDateTime(timestamp2).getHour();
//                    List<Integer> listHour = hourSlotsList(hourStart, hourEnd);
//                    List<Tuple5<Integer, Integer, Double, Double, Long>> list = new ArrayList<>();
//                    for (int i : listHour) {
//                        list.add(new Tuple5<>(i, 1, row.getDouble(TIP_AMOUNT_COL), row.getDouble(TIP_AMOUNT_COL) * row.getDouble(TIP_AMOUNT_COL), row.getLong(PAYMENT_TYPE_COL)));
//                    }
//                    return list.iterator();
//                }, Encoders.tuple(INT(), INT(), DOUBLE(), DOUBLE(), LONG()))
    //.count();

    //  System.out.println("Conteggio: " + count);
    //   }


    /*
     * Hourly distribution of
     * - mean number of trips each hour
     * - mean of tips each hour
     * - standard deviation of tips each hour
     * - most popular payment method each hour (MAX number of occurrence)
     * TODO: dovremmo raggruppare per ora, quindi avere 24 chiavi diverse.
     */
    private static JavaPairRDD query2PerHourWithGroupBy(SparkSession spark, String file) {
        JavaRDD<Row> rdd = spark.read().parquet(file).toJavaRDD();
        //
        JavaRDD<Row> filter = rdd.filter(row -> !(row.isNullAt(0) || row.isNullAt(1) || row.isNullAt(2) || row.isNullAt(3) || row.getDouble(TIP_AMOUNT_COL) < 0.0));

        JavaPairRDD<Tuple3<Integer, Integer, Long>, Tuple3<Integer, Double, Double>> mappedPair1 = filter.mapToPair(row -> {
            Timestamp timestamp = row.getTimestamp(PICKUP_COL);
            Timestamp timestamp2 = row.getTimestamp(DROPOFF_COL);
            int hourStart = Utils.toLocalDateTime(timestamp).getHour();
            int hourEnd = Utils.toLocalDateTime(timestamp2).getHour();
            // BitSet hours = hourSlots(hourStart, hourEnd);
            return new Tuple2<>(new Tuple3<>(hourStart, hourEnd, row.getLong(PAYMENT_TYPE_COL)), new Tuple3<>(1, row.getDouble(TIP_AMOUNT_COL), row.getDouble(TIP_AMOUNT_COL) * row.getDouble(TIP_AMOUNT_COL)));
            // tuple2 : ((hourStart, hourEnd), (1, tip_amount, square_tip_amount, payment_tipe))
        });

        JavaPairRDD<Tuple3<Integer, Integer, Long>, TipAndTrips> mappedPair2 = mappedPair1.mapValues(tup4 -> new TipAndTrips(tup4._1(), tup4._2(), tup4._3()));
        JavaPairRDD<Tuple3<Integer, Integer, Long>, TipAndTrips> reduced1 = mappedPair2.reduceByKey((a, b) -> new TipAndTrips(a.getTripCount() + b.getTripCount(), a.getTipAmount() + b.getTipAmount(), a.getSquareTipAmount() + b.getSquareTipAmount()));// number of elements in RDD is greatly reduced
// .count(): 386, with 8M starting elements. It takes 7 seconds with initial 3000 elements, 1m 46 s with initial 8M elements.
        // JavaPairRDD<Tuple3<Integer, Integer, Long>, Iterable<TipAndTrips>> groupByKey = reduced1.groupByKey();

        JavaPairRDD<Tuple2<Integer, Long>, TipAndTrips> flattone = reduced1.flatMapToPair(ttt -> {
            List<Tuple2<Tuple2<Integer, Long>, TipAndTrips>> list = new ArrayList();

            return list.iterator();
        });


        flattone.collect().forEach(System.out::println);
        return null;

    }


 /*   private static JavaPairRDD query2PerHour(SparkSession spark, String file) {
        JavaPairRDD<Tuple2<Integer, Integer>, Tuple4<Integer, Double, Double, Long>> rdd = spark.read().parquet(file)
                .filter(row -> {
                    return true; //!(row.isNullAt(0) || row.isNullAt(1) || row.isNullAt(2) || row.isNullAt(3));
                })
                // before mapToPair: [timestamp_pickup, timestamp_dropoff, tip_amount, payment_type]
                .mapToPair(row -> {
                    Timestamp timestamp = row.getTimestamp(PICKUP_COL);
                    Timestamp timestamp2 = row.getTimestamp(DROPOFF_COL);
                    int hourStart = Utils.toLocalDateTime(timestamp).getHour();
                    int hourEnd = Utils.toLocalDateTime(timestamp2).getHour();
                    // BitSet hours = hourSlots(hourStart, hourEnd);
                    return new Tuple2<>(new Tuple2<>(hourStart, hourEnd), new Tuple4<>(1, row.getDouble(TIP_AMOUNT_COL), row.getDouble(TIP_AMOUNT_COL) * row.getDouble(TIP_AMOUNT_COL), row.getLong(PAYMENT_TYPE_COL)));
                    // return new Tuple2<>(new Tuple2<>(hourStart, hourEnd), new Tuple2<>(row.getDouble(TIP_AMOUNT_COL), row.getLong(PAYMENT_TYPE_COL)));
                    // NEW tuple2: (hourSLOTS, (tip_amount, payment_tipe))
                    // OLD tuple2 : (hour, (1, tip_amount, square_tip_amount, payment_tipe))

                })
                // persist: ((hourStart, hourEnd), (1, tip_amount, square_tip_amount, payment_type))
                .persist(StorageLevel.MEMORY_AND_DISK_SER());

        System.out.println("============ Done persistence ============");
        JobConf j = new JobConf(spark.sparkContext().hadoopConfiguration());
        // after mapValues: ((hourStart, hourEnd), (1, tip_amount, square_tip_amount)
        JavaPairRDD<Tuple2<Integer, Integer>, Tuple3<Integer, Double, Double>> rdd2 = rdd.mapValues(tup4 -> new Tuple3<>(tup4._1(), tup4._2(), tup4._3()))
                // after reduceByKey: ((hourStart, hourEnd), (count, sum_tip_amount, sum_square_tip_amount))
                .reduceByKey((a, b) -> new Tuple3<>(a._1() + b._1(), a._2() + a._2(), a._3() + b._3()));// number of elements in RDD is greatly reduced
// .count(): 386, with 8M starting elements. It takes 7 seconds with initial 3000 elements, 1m 46 s with initial 8M elements.

        // spark.createDataFrame(rdd2, );
        rdd2.flatMap( t -> {
            // given two hours, we get a bitset of size 24 with each bit=1 representing one of the 24 hours that is included in the time slot
            // example start=00:14, end 02:35 -> Bitset [1,1,1,0,...,0]
            BitSet hours = hourSlots(t._1._1, t._1._2);
            List<Tuple2<Integer, Tuple3<Integer, Double, Double>>> tups = new ArrayList<>();
            // count of trips, sum and square_sum are copied for each hour of the hourSlot
            for (int i = 0, hourSlotNumber = hours.size(); i < hourSlotNumber; i++) {
                if (hours.get(i)) {
                    Tuple2<Integer, Tuple3<Integer, Double, Double>> tup2_3 = new Tuple2<>(
                            i, // hour
                            new Tuple3<>(
                                    t._2._1(), // count_trips copied for the hour i
                                    t._2._2(), // sum_tip copied for the hour i
                                    t._2._3()  // square_sum_tip copied for the hour i
                            ));
                    tups.add(tup2_3);
                }
            }
            // TODO: maybe... bisogna farla diventare flatMapToPair
            return tups.iterator();
        });
        // after flatMap: (hour, count, sum_tip_amount, sum_square_tip_amount). Number of elements in RDD is increased

        System.out.println("tripsAndTipsEachHour = " + tripsAndTipsEachHour);
                *//*.mapToPair(tup4 -> new Tuple2<>(tup4._1(), new Tuple3<>(tup4._2(), tup4._3(), tup4._4())))
                // after mapToPair: (hour, (trip_count, sum_tip_amount, sum_square_tip_amount))
                // now we need to sum again the values to get mean/stdev for each hour (before the sums were for the entire time slot)
                .reduceByKey((v1, v2) -> new Tuple3<>(v1._1() + v2._1(), v1._2() + v2._2(), v1._3() + v2._3()))
                // after reduceByKey: (hour, (trips, hourly_sum_tip_amount, hourly_sum_square_tip_amount))
                .mapValues(t -> new Tuple3<>(t._1(), t._2() / (double) t._1(), Utils.stddev(t._1(), t._2(), t._3())))
                // after mapValues: (hour, (trips, mean_tip_amount, stdev_tip_amount))
                .collect();*//*
//        System.out.println("=============== Im here =============== ");
//        tripsAndTipsEachHour.forEach(System.out::println);


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
        // TODO:  we don't need it anymore ???
        // rdd.unpersist();
        return null;
    }
*/

    /**
     * Returns a bitset with all hour slot between hourStart and hourEnd
     *
     * @param hourStart
     * @param hourEnd
     * @return
     */
    public static BitSet hourSlots(int hourStart, int hourEnd) {
        // hour zone are: 0,1,2,3,...,23 where 0 = 00:00-00:59, 23=23:00-23:59
        BitSet b = new BitSet(24);
        if (hourStart == hourEnd) {
            b.set(hourStart);
            return b;
        }
        int counter = hourStart;
        do {
            b.set(counter);
            counter = (counter + 1) % 24;
        } while (counter != hourEnd + 1);

        return b;
    }

    public static List<Integer> hourSlotsList(int hourStart, int hourEnd) {
        // hour zone are: 0,1,2,3,...,23 where 0 = 00:00-00:59, 23=23:00-23:59
        if (hourStart == hourEnd) {
            return Collections.singletonList(hourStart);
        }
        int counter = hourStart;
        List<Integer> b = new ArrayList<>();
        while (counter != hourEnd + 1) {
            b.add(counter);
            counter = (counter + 1) % 24;
        }
        return b;
    }

    public static boolean[] hourSlotsBoolArray(int hourStart, int hourEnd) {
        int counter = hourStart;
        boolean[] b = new boolean[24];
        while (counter != hourEnd + 1) {
            b[counter] = true;
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
