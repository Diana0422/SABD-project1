package com.sparkling_taxi;

import org.junit.jupiter.api.Test;

public class PerformanceTest {

    @Test
    public void printTime() {
        Performance.printDuration("Test 10", 10);
        Performance.printDuration("Test 1001", 1001);
        Performance.printDuration("Test 60001", 60001);
        Performance.printDuration("Test 1000000", 1000000);
        Performance.printDuration("Test 2000000", 100000000);
        Performance.printDuration("Test 2246526", 2246526);
        Performance.printDuration("Test 10215", 10215);
    }
}
