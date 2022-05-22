package com.sparkling_taxi;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class Query2Test {

    @Test
    public void hourSlotTest() {

        List<Integer> integers = Query2.hourSlots(0, 0);
        assertEquals(Collections.singletonList(0), integers);

        integers = Query2.hourSlots(0, 4);
        assertEquals(Arrays.asList(0, 1, 2, 3, 4), integers);

        integers = Query2.hourSlots(23, 2);
        assertEquals(Arrays.asList(23, 0, 1, 2), integers);
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
}
