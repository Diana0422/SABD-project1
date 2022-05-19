package com.sparkling_taxi;


import org.apache.commons.lang3.time.FastDateParser;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;

import java.io.File;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class Query1 {

    public static final String FILE_1 = "hdfs://namenode:9000/home/dataset-batch/yellow_tripdata_2021-12.parquet";
    public static final String FILE_2 = "hdfs://namenode:9000/home/dataset-batch/yellow_tripdata_2022-01.parquet";
    public static final String FILE_3 = "hdfs://namenode:9000/home/dataset-batch/yellow_tripdata_2022-02.parquet";
    public static final String FILE_Q1 = "hdfs://namenode:9000/home/dataset-batch/Query1.parquet";
    public static final String FILE_CIAO = "hdfs://namenode:9000/home/dataset-batch/ciao.txt";
    public static final String OUT_DIR = "hdfs://namenode:9000/home/dataset-batch/output-query1/";
    public static final String DIR_TIMESTAMP = "hdfs://namenode:9000/home/dataset-batch/timestamp";
    public static final int PASSENGER_COUNT_COL = 2;
    public static final int DROP_OUT_COL = 1;

    /**
     * @return true if on windows is installed C:\\Hadoop\\hadoop-2.8.1\\bin\\WINUTILS.EXE
     */
    private static boolean windowsCheck() {
        if (System.getProperty("os.name").equals("windows")) {
            System.out.println("Hi");
            if (new File("C:\\Hadoop\\hadoop-2.8.1").exists()) {
                System.setProperty("hadoop.home.dir", "C:\\Hadoop");
            } else {
                System.out.println("Install WINUTIL.EXE from and unpack it in C:\\Windows");
                return false;
            }
        }
        return true;
    }

    /**
     * A working example that prints the first row of a parquet file
     */
    private static void workingJob() {
        try (SparkSession spark = SparkSession
                .builder()
                .master("spark://spark:7077")
                .appName("Query1")
                .getOrCreate();
        ) {
            // spark.sparkContext().setLogLevel("WARN");
            System.out.println("======================= before batch =========================");
            JavaRDD<Row> batch1 = spark.read().format("parquet").load(FILE_1).toJavaRDD();
            System.out.println("======================= before map =========================");
            JavaRDD<String> stringArrRDD = batch1.map(row -> {
                StringBuilder sb = new StringBuilder();
                int length = row.length();
                for (int i = 0; i < length; i++) {
                    sb.append(row.get(i) != null ? row.get(i).toString() : "null").append("\t");
                }
                return sb.toString();
            });

            System.out.println(stringArrRDD.first());

            System.out.println("====<<<>>0>=>=>===>==== Done ======>=>><><>>0<00>>==>=>=");
        }
    }

    public static void singleMonthMean(SparkSession spark, String file, List<Tuple2<String, Double>> means) {
        System.out.println("======================= before passengers =========================");
        JavaRDD<Double> passengers = spark.read().parquet(file)
                .toJavaRDD()
                .map(row -> {
                    try {
                        return row.getDouble(PASSENGER_COUNT_COL);
                    } catch (NullPointerException e) {
                        return 0.0;
                    }
                })
                .persist(StorageLevel.MEMORY_AND_DISK_SER());
        //salva in memoria serializzando (occupa meno RAM), ma se manca lo spazio, salva su disco.

        System.out.println("======================= before mean =========================");
        Double sum = passengers.reduce(Double::sum);
        long num = passengers.count();
        System.out.println("number of elements: " + num);
        System.out.println("number of passengers: " + sum);
//        Double mean = passengers.reduce(Double::sum)/passengers.count();
        Double mean = sum / (double) num;
        System.out.println("Mean number of passengers 2021-12 - 2022-02: " + mean);
        means.add(new Tuple2<>(Arrays.asList(file.split("/")).get(file.split("/").length - 1), mean));
    }

    public static JavaPairRDD<Integer, Double> multiMonthMean(SparkSession spark, String file) {
        System.out.println("======================= before passengers =========================");
        JavaPairRDD<Timestamp, Double> passengersTS = spark.read().parquet(file)
                .toJavaRDD()
                .filter(row -> !(row.isNullAt(DROP_OUT_COL) || row.isNullAt(PASSENGER_COUNT_COL))) //TODO: questo andrebbe fatto su NiFi
                .mapToPair(row -> new Tuple2<>(row.getTimestamp(DROP_OUT_COL), row.getDouble(PASSENGER_COUNT_COL)));
        // JavaRDD<Row> -> JavaRDD<Timestamp, n_passengers>


        // TODO: Puoi provare con questo
        //

        // Vedo cosa ha scritto su hdfs (se troppo lento commenta)
        // recupera dal namenode: hdfs dfs -get /home/dataset-batch/timestamp/part... /home/result.csv
        // recupera dall'host: docker cp namenode:/home/result.csv ./result.csv
        spark.createDataset(JavaRDD.toRDD(passengersTS.keys()), Encoders.TIMESTAMP()).limit(10)
                .write().csv(DIR_TIMESTAMP);

        // TODO: Questo può essere sostituito al precedente mapToPair, ma ho separato per salvare i csv su hdfs
        //    L'errore è sicuramente in getMonth(), prova a usare qualcosa tipo SimpleDateParse o FastDateParser
        JavaPairRDD<Integer, Double> passengers = passengersTS.mapToPair(tup -> new Tuple2<>(tup._1.getMonth(), tup._2))
                .persist(StorageLevel.MEMORY_AND_DISK_SER());
        // Saves in-memory serialized (less RAM occupied, more CPU used) and if RAM is full, save the remaining data on disk (serialized)

        System.out.println("======================= before num passengers =========================");
        // Counts passengers for each month
        Map<Integer, Long> num = passengers.countByKey(); // Map è la classe Java standard
        //TODO: se la prof ci chiede altre medie, il countByKey comunque va fatto una sola volta!!!
        System.out.println("======================= before reduce by key =========================");
        JavaPairRDD<Integer, Double> sum = passengers.reduceByKey(Double::sum); // (monthKey, passengerSum): only 3 tuples

        passengers.keys().distinct().collect().forEach(System.out::println);

        return sum.mapToPair(tup -> new Tuple2<>(tup._1, tup._2 / num.get(tup._1)));
    }


    public static void monthMean(SparkSession spark, String file, List<Tuple2<String, Double>> means) {
        System.out.println("======================= before passengers =========================");
        JavaRDD<Double> passengers = spark.read().parquet(file)
                .toJavaRDD()
                .map(row -> {
                    try {
                        return row.getDouble(3);
                    } catch (NullPointerException e) {
                        return 0.0;
                    }
                }).cache();
        System.out.println("======================= before mean =========================");
        Double sum = passengers.reduce(Double::sum);
        long num = passengers.count();
        System.out.println("number of elements: " + num);
        System.out.println("number of passengers: " + sum);
//        Double mean = passengers.reduce(Double::sum)/passengers.count();
        Double mean = sum / (double) num;
        System.out.println("Mean number of passengers 2021-12: " + mean);
        means.add(new Tuple2<>(Arrays.asList(file.split("/")).get(file.split("/").length - 1), mean));
    }

    public static void main(String[] args) {
        // TODO: chiamare NiFi da qui

        // Must install WINUTILS.EXE for Windows.
        // windowsCheck();

        try (SparkSession spark = SparkSession
                .builder()
                .master("spark://spark:7077")
                .appName("Query1")
                .getOrCreate();
             // JavaSparkContext sc = JavaSparkContext.fromSparkContext(spark.sparkContext());
        ) {
            spark.sparkContext().setLogLevel("WARN");

            // List<Tuple2<String, Double>> means = new ArrayList<>();
            /*String[] files = {FILE_1, FILE_2, FILE_3};
            int size = files.length;
            for (int i = 0; i < size; i++) {
                String file = files[i];
                Tuple2<String, Double> res = singleMonthMean(spark, file);
                String fileWithoutExt = FileUtils.getFileNameWithoutExtension(res._1);
                String yearMonth = Utils.getYearMonthString(fileWithoutExt);
                System.out.printf("Mean number of passengers %s: %g\n", yearMonth, res._2);
                means.add(res);
            }*/

            // Performance.measure("Query1 - file1", () -> singleMonthMean(spark, FILE_1, means));
            // Performance.measure("Query2 - file2", () -> singleMonthMean(spark, FILE_2, means));
            // Performance.measure("Query3 - file3", () -> singleMonthMean(spark, FILE_3, means));
//            JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());
//            sc.close();

            JavaPairRDD<Integer, Double> javaPairRDD = Performance.measure("Query4 - file4", () -> multiMonthMean(spark, FILE_Q1));
            javaPairRDD.collect().forEach(x -> System.out.println("Month: " + x._1 + " mean passengers" + x._2));
//
//            JavaRDD<Tuple2<String, Double>> result = sc.parallelize(means);
//            result.collect().forEach(System.out::println);
        }


    }
}
