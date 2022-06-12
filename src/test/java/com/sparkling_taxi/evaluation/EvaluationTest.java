package com.sparkling_taxi.evaluation;

import org.junit.jupiter.api.Test;
import scala.Tuple2;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class EvaluationTest {
    @Test
    public void meanStdevTest() throws IOException {
        Time t = new Time(23, 103);
        Evaluation e = new Evaluation("", 3);
        List<Time> times = new ArrayList<>();
        times.add(t);
        times.add(t);
        times.add(t);
        Tuple2<Time, Time> timeTimeTuple2 = e.calculateMeanStdev(times);
        assertEquals(timeTimeTuple2._1, t);
        assertEquals(new Time(0, 0), timeTimeTuple2._2);

        times.removeIf(time -> true);

        times.add(t);
        times.add(new Time(24, 103));
        System.out.println(times);
        System.out.println(times.get(0).toMillis());
        System.out.println(times.get(1).toMillis());
        Tuple2<Time, Time> ttt = e.calculateMeanStdev(times);
        assertEquals(new Time(23, 603), ttt._1);
        assertEquals(new Time(500), ttt._2);
        e.closeWriter();
    }


}
