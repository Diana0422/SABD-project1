package com.sparkling_taxi;

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

    /**
     * Restituisce la data all' inizio della giornata a partire da una stringa yyyy-MM-dd
     * @param jiraDate data che CONTIENE il formato yyyy-MM-dd
     * @return la corrispondente Date all' inizio della giornata.
     */
//    public static Date getDateFromYearMonthDayString(String jiraDate){
//        // Pattern-Matching della data
//        var datePattern = Pattern.compile("([0-9]{4})-([0-9]{2})-([0-9]{2})");
//        var dateMatcher = datePattern.matcher(jiraDate);
//        if (dateMatcher.find()) {
//            // Il gruppo zero corrisponde all' intera stringa trovata
//            String year = dateMatcher.group(1);
//            String month = dateMatcher.group(2);
//            String day = dateMatcher.group(3);
//
//            // Rimuovo i leading zero per il mese e per il giorno
//            if(month.charAt(0) == '0') month = month.substring(1);
//            if(day.charAt(0) == '0') day = day.substring(1);
//
//            Calendar calendar = Calendar.getInstance();
//
//            calendar.set(Calendar.YEAR, Integer.parseInt(year));
//            calendar.set(Calendar.MONTH, Integer.parseInt(month) - 1); // i mesi partono da 0 in Calendar
//            calendar.set(Calendar.DAY_OF_MONTH, Integer.parseInt(day));
//            return atStartOfDay(calendar.getTime());
//        }
//        return null;
//    }
//
//    public static Date atStartOfDay(Date d){
//        return toDate(toLocalDate(d));
//    }
//
//    public static Date toDate(LocalDateTime localDate) {
//        return toDate(localDate.toLocalDate());
//    }
//
//    public static LocalDate toLocalDate(Date date) {
//        return new LocalDate()LocalDate.ofInstant(date.toInstant(), ZoneId.systemDefault());
//    }

    /**
     * Restituisce la data all' inizio della giornata a partire da una stringa yyyy-MM-dd
     *
     * @param jiraDate data che CONTIENE il formato yyyy-MM-dd
     * @return la corrispondente Date all' inizio della giornata.
     */
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
}
