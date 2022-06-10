package com.sparkling_taxi.sparksql;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class QuerySQL3Test {
    @Test
    public void getFiveTest() {

        List<Integer> nothing = new ArrayList<>();
        // with negative offset returns an empty list
        List<Integer> five = QuerySQL3.getFive(nothing, -1);
        assertTrue(five.isEmpty());
        System.out.println("with a empty list returns an empty list!");

        // with a empty list returns an empty list.
        five = QuerySQL3.getFive(nothing, 10);
        assertTrue(five.isEmpty());
        System.out.println("with a empty list returns an empty list!");

        // with a list with 5 elements and offset 0, returns first 5 elements
        List<Integer> integers = Arrays.asList(1, 2, 3, 4, 5);
        five = QuerySQL3.getFive(integers, 0);
        assertEquals(5, five.size(), "Not size 5");
        assertEquals(integers, five, "Not right elements");
        System.out.println("with a list with 5 elements and offset 0, returns first 5 elements");

        // with a list less than  5 elements and offset 0, returns all elements
        integers = Arrays.asList(1, 2, 3);
        five = QuerySQL3.getFive(integers, 0);
        assertEquals(3, five.size(), "Not size 3");
        assertEquals(integers, five, "Not right elements");
        System.out.println("with a list with 3 elements and offset 0, returns first 3 elements");

        // with a list less than 5 elements and offset 1, returns nothing
        integers = Arrays.asList(1, 2, 3);
        five = QuerySQL3.getFive(integers, 1);
        assertTrue(five.isEmpty(), "Not empty");
        System.out.println("with a list with 3 elements and offset 1, returns empty list");

        // with a list with 10 elements and offset 0, returns first 5 elements
        integers = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
        five = QuerySQL3.getFive(integers, 0);
        assertEquals(5, five.size(), "Not size 5");
        assertEquals(Arrays.asList(1, 2, 3, 4, 5), five, "Not right elements");
        System.out.println("with a list with 10 elements and offset 0, returns first 5 elements");

        // with a list with 10 elements and offset 1, returns second 5 elements
        five = QuerySQL3.getFive(integers, 1);
        assertEquals(5, five.size(), "Not size 5");
        assertEquals(Arrays.asList(6, 7, 8, 9, 10), five, "Not right elements");
        System.out.println("with a list with 10 elements and offset 1, returns second 5 elements");

        // with a list with 10 elements and offset 2+, returns an emptyList
        five = QuerySQL3.getFive(integers, 3);
        assertTrue(five.isEmpty());
        System.out.println("with a list with 10 elements and offset 2+, returns an emptyList");

        // with a list with not multiple of 5 elements, the last offset return only the remaining elements
        five = QuerySQL3.getFive(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9), 0);
        assertEquals(5, five.size(), "Not size 5");
        assertEquals(Arrays.asList(1, 2, 3, 4, 5), five, "Not right elements");

        five = QuerySQL3.getFive(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9), 1);
        assertEquals(4, five.size(), "Not size 4");
        assertEquals(Arrays.asList(6, 7, 8, 9), five, "Not right elements");
        System.out.println("with a list with not multiple of 5 elements, the last offset return only the remaining elements");


    }

}
