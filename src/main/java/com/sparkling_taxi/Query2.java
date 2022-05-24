package com.sparkling_taxi;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;
import scala.Tuple4;
import scala.collection.EvidenceIterableFactory$;

import java.sql.Timestamp;
import java.util.*;
import java.util.stream.Collectors;

import static com.sparkling_taxi.Utils.intRange;

// docker cp backup/Query2.parquet namenode:/home/Query2.parquet
// hdfs dfs -put Query2.parquet /home/dataset-batch/Query2.parquet
public class Query2 {

    public static final String FILE_Q2 = "hdfs://namenode:9000/home/dataset-batch/Query2.parquet";
    // public static final String FILE_Q2 = "hdfs://namenode:9000/home/dataset-batch/LittleQuery2.parquet";
    public static final String PARTIAL_OUTPUT = "hdfs://namenode:9000/home/dataset-batch/partOutputQ2";
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

        JavaPairRDD<TripleKey, TipAndTrips> mappedPair1 = filter.mapToPair(row -> {
            Timestamp timestamp = row.getTimestamp(PICKUP_COL);
            Timestamp timestamp2 = row.getTimestamp(DROPOFF_COL);
            int hourStart = Utils.toLocalDateTime(timestamp).getHour();
            int hourEnd = Utils.toLocalDateTime(timestamp2).getHour();
            return new Tuple2<>(new TripleKey(hourStart, hourEnd, row.getLong(PAYMENT_TYPE_COL)), new TipAndTrips(1, row.getDouble(TIP_AMOUNT_COL), row.getDouble(TIP_AMOUNT_COL) * row.getDouble(TIP_AMOUNT_COL)));
        });

        // TODO: non salva i risultati parziali
        JavaPairRDD<TripleKey, TipAndTrips> ttt1 = mappedPair1.reduceByKey(TipAndTrips::sumWith);// number of elements in RDD is greatly reduced
        JavaPairRDD<DoubleKey, TipAndTrips> flattone = ttt1.flatMapToPair(ttt -> {
            List<Tuple2<DoubleKey, TipAndTrips>> list = new ArrayList<>();
            int startHour = ttt._1.getHourStart();
            int stopHour = ttt._1.getHourEnd();
            List<Integer> integers = hourSlotsList(startHour, stopHour);
            for (int hour : integers) {
                Tuple2<DoubleKey, TipAndTrips> tuple = new Tuple2<>(new DoubleKey(hour, ttt._1.getPaymentType()), ttt._2);
                list.add(tuple);
            }
            return list.iterator();
        });

        flattone.reduceByKey(TipAndTrips::sumWith)
                .mapToPair(d -> new Tuple2<>(d._1.getHour(), d._2.toTipTripsAndPayment(d._1.getPaymentType())))
                .reduceByKey(TipTripsAndPayment::sumWith)
                .collect().forEach(System.out::println);

        return null;

    }

    /**
     * Returns a bitset with all hour slot between hourStart and hourEnd
     *
     * @param hourStart
     * @param hourEnd
     * @return
     */
    @Deprecated
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
        } else if (hourStart < hourEnd) {
            return intRange(hourStart, hourEnd + 1);
        } else {
            List<Integer> integers = intRange(hourStart, 24);
            integers.addAll(intRange(0, hourEnd + 1));
            return new ArrayList<>(new HashSet<>(integers));
        }
    }

    @Deprecated
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
