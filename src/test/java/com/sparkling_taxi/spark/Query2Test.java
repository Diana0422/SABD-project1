package com.sparkling_taxi.spark;

import org.junit.jupiter.api.Test;

import java.util.*;

import static com.sparkling_taxi.utils.Utils.intRange;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class Query2Test {

    @Test
    public void hourSlotListTest() {
        List<Integer> list = Query2.hourSlotsList(0, 0);

        assertEquals(Collections.singletonList(0), list);

        list = Query2.hourSlotsList(0, 4);
        assertEquals(Arrays.asList(0, 1, 2, 3, 4), list);

        list = Query2.hourSlotsList(23, 2);
        assertEquals(Arrays.asList(0, 1, 2, 23), list);

        list = Query2.hourSlotsList(14, 12);
        assertEquals(Arrays.asList(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23), list);

        list = Query2.hourSlotsList(23, 23);
        assertEquals(Collections.singletonList(23), list);

        list = Query2.hourSlotsList(0, 23);
        assertEquals(Arrays.asList(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12,13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23), list);
    }

    @Test
    public void rangeTest() {
        assertEquals(Collections.singletonList(0), intRange(0, 0));
        assertEquals(Arrays.asList(0, 1), intRange(0, 1));
        assertEquals(Arrays.asList(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 14, 15, 16, 17, 18, 19, 20, 21, 22), intRange(0, 23));
    }
}
