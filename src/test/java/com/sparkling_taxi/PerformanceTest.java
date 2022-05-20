package com.sparkling_taxi;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class PerformanceTest {

    @Test
    public void printTime() {
        assertEquals("Duration Test 10: 10 milliseconds", Performance.printDuration("Test 10", 10));
        assertEquals("Duration Test 1001: 1 s 1 ms", Performance.printDuration("Test 1001", 1001));
        assertEquals("Duration Test 60001: 1 m 0 s 1 ms", Performance.printDuration("Test 60001", 60001));
        assertEquals("Duration Test 1000000: 16 m 40 s 0 ms", Performance.printDuration("Test 1000000", 1000000));
        assertEquals("Test 2000000 is Done! It took more than one hour!", Performance.printDuration("Test 2000000", 100000000));
        assertEquals("Duration Test 2246526: 37 m 26 s 526 ms", Performance.printDuration("Test 2246526", 2246526));
        assertEquals("Duration Test 10215: 10 s 215 ms", Performance.printDuration("Test 10215", 10215));
    }
}
