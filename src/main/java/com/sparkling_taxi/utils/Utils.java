package com.sparkling_taxi.utils;

import com.sparkling_taxi.nifi.NifiTemplateInstance;
import scala.Tuple2;

import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.sparkling_taxi.utils.FileUtils.hasFileHDFS;
import static com.sparkling_taxi.utils.Const.*;

public class Utils {

    public static boolean isWindows() {
        return System.getProperty("os.name").equals("windows");
    }

    public static boolean isLinux() {
        return System.getProperty("os.name").equals("linux");
    }

    public static String getYearMonthDayString(String with_yyyy_MM_dd) {

        Pattern datePattern = Pattern.compile("([0-9]{4})-([0-9]{2})-([0-9]{2})");
        Matcher dateMatcher = datePattern.matcher(with_yyyy_MM_dd);
        if (dateMatcher.find()) {
            return dateMatcher.group();
        }
        return "UNKNOWN_DATE";
    }

    public static String getYearMonthString(String with_yyyyMM) {
        Pattern datePattern = Pattern.compile("([0-9]{4})-([0-9]{2})");
        Matcher matcher = datePattern.matcher(with_yyyyMM);
        if (matcher.find()) {
            return matcher.group();
        }
        return "UNKNOWN_MONTH";
    }

    public static LocalDateTime toLocalDateTime(Timestamp t) {
        Date date = new Date(t.getTime());
        LocalDateTime ld = date.toInstant().atZone(ZoneId.systemDefault()).toLocalDateTime();
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd.HH");
        ld.format(formatter);
        return ld;
    }

    public static String getDay(Timestamp t){
        Date date = new Date(t.getTime());
        LocalDate ld = date.toInstant().atZone(ZoneId.systemDefault()).toLocalDate();
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
        return ld.format(formatter);
    }

    /**
     * Calculates stdev using the formula
     *            sqrt(E[X^2] - E^2[X])
     * where:
     *   E[X] = valueSum / count
     *   E[X^2] = squareSum / count
     *   E^2[X] = (valueSum / count)^2
     *   Var(X) = E[X^2] - E^2[X]
     *   stdev(X) = sqrt(Var(X))
     * @param count the number of elements in the sample
     * @param valueSum the sum of values of the sample
     * @param squareSum the sum of square values of the sample
     * @return the standard deviation of the sample
     */
    public static double stddev(double count, double valueSum, double squareSum) {
        return Math.sqrt((squareSum / count - (valueSum / count) * (valueSum / count)));
    }

    public static Tuple2<Time, Time> calculateMeanStdev(List<Time> t){
        long count = 0;
        long totalMillis = 0;
        long squareTotalMillis = 0;
        for (Time time : t) {
            long millis = time.toMillis();
            totalMillis += millis;
            squareTotalMillis += millis * millis;
            count++;
        }

        double mean = (double) totalMillis / count;
        double stdev = Utils.stddev((double) count, (double) totalMillis, (double) squareTotalMillis);

        return new Tuple2<>(new Time((long) mean), new Time((long) stdev));
    }

    /**
     * Starting from the sum of *only* prime numbers, gets the prime number which has the maximum exponent.
     * USED TO GET THE MAX TYPE
     *
     * @param sumOfPrimesTypes
     * @return prime number with maximum exponent
     */
    public static int argMax(final int sumOfPrimesTypes, List<Integer> primes) {
        Map<Integer, Integer> map = new HashMap<>();
        for (Integer prime : primes) {
            map.put(prime, sumOfPrimesTypes / prime);
        }


        return primes.stream()
                .map(prime -> new Tuple2<>(prime, sumOfPrimesTypes))
                .map(tup -> tup._2 / tup._1)
                .max((o1, o2) -> {
                    if (o1 > o2) return o2;
                    else return o1;
                }).map(primes::get)
                .orElse(primes.get(0));
    }

    public static List<Integer> intRange(int start, int end){
        if (start < end){
            List<Integer> l = new ArrayList<>();
            for (int i = start; i<end; i++){
                l.add(i);
            }
            return l;
        } else if (start == end){
            return Collections.singletonList(start);
        } else {
            throw new IllegalStateException("start > end!!!");
        }
    }

    /**
     * Downloads all files of the dataset to HDFS, if they not exists, using Nifi.
     * Also downloads the file with mappings between Zone Ids and Names.
     */
    public static void downloadFilesIfNeeded(){
        // check if input dataset are already downloaded
        boolean allDownloaded = true;
        for (int i = 0; i < FILE_TO_DOWNLOAD.length; i++) {
            allDownloaded = allDownloaded && FileUtils.hasFileHDFS(FILE_TO_DOWNLOAD[i]);
        }
        if (!allDownloaded){
            // if not, start downloading them
            NifiTemplateInstance n = new NifiTemplateInstance(Const.DOWNLOAD_TEMPLATE, NIFI_URL);
            n.uploadAndInstantiateTemplate();
            n.runAll();
            // wait until all files are downloaded
            do{
                try {
                    Thread.sleep(5000);
                    System.out.println("Waiting for files to download");
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                allDownloaded = true;
                for (int i = 0; i < FILE_TO_DOWNLOAD.length; i++) {
                    allDownloaded = allDownloaded && FileUtils.hasFileHDFS(FILE_TO_DOWNLOAD[i]);
                }
            } while (!allDownloaded);
            // now cleanup nifi
            n.stopAll();
            n.removeAll();
            System.out.println("All 3 input files downloaded!");
        } else {
            System.out.println("The datasets are already downloaded!");
        }
    }

    public static void doPreProcessing(String file_for_query, String preprocessing_template_for_query){
        downloadFilesIfNeeded();
        if (!hasFileHDFS(file_for_query)) {
            NifiTemplateInstance n = new NifiTemplateInstance(preprocessing_template_for_query, NIFI_URL);
            n.uploadAndInstantiateTemplate();
            n.runAll();
            do {
                System.out.println("Waiting for preprocessing to complete...");
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            } while (!hasFileHDFS(file_for_query));
            n.stopAll();
            n.removeAll();
        }
    }
}
