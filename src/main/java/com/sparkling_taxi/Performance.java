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

    public static <V> V measure(Callable<V> r) {
        return measure("", r);
    }

    public static <V> V measure(String message, Callable<V> r) {
        Instant start = Instant.now();
        V call = null;
        try {
            call = r.call();
        } catch (Exception e) {
            logger.warning("Failed to compute performance.");
        }
        Instant end = Instant.now();
        printDuration(message, start, end);
        return call;
    }

    public static void measure(Runnable r) {
        measure("", r);
    }

    public static void measure(String message, Runnable r) {
        Instant start = Instant.now();
        try {
            r.run();
        } catch (Exception e) {
            logger.warning("Failed to compute performance.");
        }
        Instant end = Instant.now();
        printDuration(message, start, end);
    }

    //TODO: not tested in Java 8
    private static void printDuration(String s, Instant start, Instant end) {
        Duration duration = Duration.between(start, end);
        if (duration.toNanos() < 1000) {
            Logger.getAnonymousLogger().info(() -> String.format("Duration %s: %d nanoseconds (10^-9)", s, duration.toNanos()));
        } else if (duration.toMillis() < 1000) {
            Logger.getAnonymousLogger().info(() -> String.format("Duration %s: %d milliseconds (10^-3)", s, duration.toMillis()));
        } else if (duration.getSeconds() < 60) {
            Logger.getAnonymousLogger().info(() -> String.format("Duration %s: %d s %d ms", s, duration.getSeconds(), duration.toMillis()));
        } else if (duration.toMinutes() < 60) {
            Logger.getAnonymousLogger().info(() -> String.format("Duration %s: %d min %d s", s, duration.toMinutes(), duration.getSeconds()));
        } else {
            Logger.getAnonymousLogger().info(() -> String.format("Duration %s: %d h %d min %d s", s, duration.toHours(), duration.toMinutes(), duration.getSeconds()));
        }
    }

}
