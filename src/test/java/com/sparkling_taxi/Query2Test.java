package com.sparkling_taxi;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class Query2Test {

    @Test
    public void hourSlotTest() {
        int[] integers = Query2.hourSlots(0, 0);

        List<Integer> list = convertList(integers);
        assertEquals(Collections.singletonList(0), list);

        integers = Query2.hourSlots(0, 4);
        list = convertList(integers);
        assertEquals(Arrays.asList(0, 1, 2, 3, 4), list);

        integers = Query2.hourSlots(23, 2);
        list = convertList(integers);
        assertEquals(Arrays.asList(0, 1, 2, 23), list);
    }

    private List<Integer> convertList(int[] integers) {
        List<Integer> list = new ArrayList<>();
        for (int i = 0; i < integers.length; i++) {

            if (integers[i] == 1) list.add(i);
        }
        return list;
    }

    @Test
    public void maxPrimeExponentTest() {
        int max = Utils.argMax(2 + 2 + 2 + 3 + 3 + 5 + 5 + 5 + 5 + 5, Arrays.asList(2, 3, 5));
        assertEquals(5, max);

        max = Utils.argMax(2 + 2 + 2 + 3 + 3 + 5 + 5 + 5 + 5 + 5 + 7 + 7 + 7 + 7 + 7 + 7 + 7 + 7 + 7 + 7, Arrays.asList(2, 3, 5, 7));
        assertEquals(7, max);

        max = Utils.argMax(2 + 2 + 2 + 3 + 3 + 3 + 5 + 5, Arrays.asList(2, 3, 5));
        assertTrue(max == 2 || max == 3);

    }

//    @Test
//    public void maxPrimeExponentTestWithProd() {
//        int max = Utils.argMaxWithMul(2 * 2 * 2 * 3 * 3 * 5 * 5 * 5 * 5 * 5, Arrays.asList(2, 3, 5));
//        assertEquals(5, max);
//
//        max = Utils.argMaxWithMul(2 * 2 * 2 * 3 * 3 * 5 * 5 * 5 * 5 * 5 * 7 * 7 * 7 * 7 * 7 * 7 * 7 * 7 * 7 * 7, Arrays.asList(2, 3, 5, 7));
//        assertEquals(7, max);
//
//        max = Utils.argMaxWithMul(2 * 2 * 2 * 3 * 3 * 3 * 5 * 5, Arrays.asList(2, 3, 5));
//        assertTrue(max == 2 || max == 3);
//
//    }
}
