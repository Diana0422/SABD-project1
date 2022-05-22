package com.sparkling_taxi;

import scala.Tuple2;

import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Utils {

    public static final String LINUX_SEPARATOR = "/";
    public static final String WINDOWS_SEPARATOR = "\\";


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

    public static double stddev(double count, double valueSum, double squareSum) {
        return Math.sqrt((squareSum / count - (valueSum / squareSum) * (valueSum / squareSum)) / count);
    }

    /**
     * Starting from the sum of *only* prime numbers, gets the prime number which has the maximum exponent.
     * USED TO GET THE MAX TYPE
     *
     * @param sumOfPrimesTypes
     * @return prime number with maximum exponent
     */
    public static int argMax(int sumOfPrimesTypes, List<Integer> primes) {
        return primes.stream().map(prime -> new Tuple2<>(prime, sumOfPrimesTypes / prime))
                .max((o1, o2) -> {
                    if (o1._2 > o2._2) return o2._1;
                    else return o1._1;
                }).map(t -> t._1)
                .orElse(primes.get(0));
    }
}
