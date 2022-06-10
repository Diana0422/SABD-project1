package com.sparkling_taxi.evaluation;

import com.sparkling_taxi.bean.QueryResult;
import com.sparkling_taxi.spark.Query;
import com.sparkling_taxi.spark.Query1;
import com.sparkling_taxi.spark.Query2;
import com.sparkling_taxi.spark.Query3;
import com.sparkling_taxi.sparksql.QuerySQL1;
import com.sparkling_taxi.sparksql.QuerySQL2;
import com.sparkling_taxi.sparksql.QuerySQL3;
import com.sparkling_taxi.utils.Utils;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

import static com.sparkling_taxi.utils.Const.NUM_EVALUATIONS;

public class Evaluation {

    public static void main(String[] args) {
        Query1 q1 = new Query1();
        printResults(evaluate(q1), q1);

        Query2 q2 = new Query2();
        printResults(evaluate(q2), q2);

        Query3 q3 = new Query3();
        printResults(evaluate(q3), q3);

        QuerySQL1 qs1 = new QuerySQL1();
        printResults(evaluate(qs1), qs1);

        QuerySQL2 qs2 = new QuerySQL2();
        printResults(evaluate(qs2), qs2);

        QuerySQL3 qs3 = new QuerySQL3();
        printResults(evaluate(qs3), qs3);
    }

    public static <T extends QueryResult> Tuple2<Time, Time> evaluate(Query<T> q) {
        q.preProcessing();
        List<Time> timesQuery1 = new ArrayList<>();
        for (int i = 0; i < NUM_EVALUATIONS; i++) {
            Time time = Performance.measureTime(q::processing);
            timesQuery1.add(time);
        }
        Tuple2<Time, Time> timeTimeTuple2 = calculateMeanStdev(timesQuery1);
        q.closeSession();
        return timeTimeTuple2;
    }

    public static <T extends QueryResult> void printResults(Tuple2<Time, Time> tt, Query<T> q) {
        System.out.println("================= " + q.getClass().getSimpleName() + " ==========================");
        System.out.println("Number of Runs: " + NUM_EVALUATIONS + " Average Time: " + tt._1 + " Standard Deviation: " + tt._2);
    }

    public static Tuple2<Time, Time> calculateMeanStdev(List<Time> t) {
        long count = 0;
        long totalMillis = 0;
        long squareTotalMillis = 0;
        for (Time time : t) {
            long millis = time.toMillis();
            totalMillis += millis;
            squareTotalMillis += millis * millis;
            count++;
        }

        double mean = (double) totalMillis / count;
        double stdev = Utils.stddev((double) count, (double) totalMillis, (double) squareTotalMillis);

        return new Tuple2<>(new Time((long) mean), new Time((long) stdev));
    }
}
