package com.sparkling_taxi.utils;

import com.sparkling_taxi.nifi.NifiTemplateInstance;

import java.io.File;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.sparkling_taxi.utils.FileUtils.deleteFromHDFS;
import static com.sparkling_taxi.utils.FileUtils.hasFileHDFS;
import static com.sparkling_taxi.utils.Const.*;

public class Utils {

    public static boolean isWindows() {
        return System.getProperty("os.name").equals("windows");
    }

    public static boolean isLinux() {
        return System.getProperty("os.name").equals("linux");
    }

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
    public static String getHourDay(Timestamp t){
        Date date = new Date(t.getTime());
        LocalDateTime ld = date.toInstant().atZone(ZoneId.systemDefault()).toLocalDateTime();
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd.HH:00");
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
            System.out.println("All input files downloaded!");
        } else {
            System.out.println("The datasets are already downloaded!");
        }
    }

    public static void doPreProcessing(String file_for_query, String preprocessing_template_for_query, boolean force){
        downloadFilesIfNeeded();
        if (force){
            deleteFromHDFS(file_for_query);
        }
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
