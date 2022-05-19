package com.sparkling_taxi;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.Callable;
import java.util.logging.Logger;

/**
 * Classe di utilità che serve a misurare il tempo impiegato da un blocco di codice
 */
public class Performance {
    private static final Logger logger = Logger.getLogger(Performance.class.getSimpleName());

    private Performance() {
    }

    private static long start() {
        return System.currentTimeMillis();
    }

    private static long elapsed(long start) {
        return System.currentTimeMillis() - start;
    }

    public static <V> V measure(Callable<V> r) {
        return measure("", r);
    }

    public static <V> V measure(String message, Callable<V> r) {
        long start = start();
        V call = null;
        try {
            call = r.call();
        } catch (Exception e) {
            System.out.println("Failed to compute performance." + e.getMessage());
        }
        long elapsed = elapsed(start);
        printDuration(message, elapsed);
        return call;
    }

    public static void measure(Runnable r) {
        measure("", r);
    }

    public static void measure(String message, Runnable r) {
        long s = start();
        try {
            r.run();
        } catch (Exception e) {
            logger.warning("Failed to compute performance."  + e.getMessage());
        }
        long elapsed = elapsed(s);
        printDuration(message, elapsed);
    }

    static void printDuration(String s, long elapsed) {
        if (elapsed < 1000) {
            System.out.printf("Duration %s: %d milliseconds\n", s, elapsed);
        } else if (elapsed / 1000 < 60) {
            long seconds = elapsed / 1000;
            System.out.printf("Duration %s: %d s %d ms\n", s, seconds, elapsed - seconds * 1000);
        } else if (elapsed / 60000 < 60) {
            long minutes = elapsed / 60000;
            long seconds = (elapsed - minutes * 60000) / 1000;
            System.out.printf("Duration %s: %d m %d s %d ms\n", s, minutes, seconds, elapsed - minutes * 60000 - seconds * 1000);
        } else {
            System.out.println(s + " is Done! It took more than one hour!");
        }
    }

}
