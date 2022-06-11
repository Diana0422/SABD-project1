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

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static com.sparkling_taxi.utils.Const.EVALUATION_FILE;
import static com.sparkling_taxi.utils.Const.NUM_EVALUATIONS;

public class Evaluation {

    public static void main(String[] args) {
        List<Query<? extends QueryResult>> queries = Arrays.asList(new Query1(), new Query2(), new Query3(), new QuerySQL1(), new QuerySQL2(), new QuerySQL3());
        List<EvalResult> evalResults = new ArrayList<>();
        for (Query<? extends QueryResult> query : queries) {
            Tuple2<Time, Time> eval1 = evaluate(query);
            printResults(eval1, query);
            evalResults.add(new EvalResult(query.getClass().getSimpleName(), eval1._1, eval1._2));
            System.out.println("======================================================================");
        }
        saveToCSV(evalResults);
    }

    public static <T extends QueryResult> Tuple2<Time, Time> evaluate(Query<T> q) {
        q.preProcessing();
        List<Time> timesQuery1 = new ArrayList<>();
        for (int i = 0; i < NUM_EVALUATIONS; i++) {
            Time time = Performance.measureTime(q::processing);
            timesQuery1.add(time);
            System.out.println("Run " + (i + 1) + "/" + NUM_EVALUATIONS + " time: " + time);
        }
        Tuple2<Time, Time> timeTimeTuple2 = calculateMeanStdev(timesQuery1);
        q.closeSession();
        return timeTimeTuple2;
    }

    public static <T extends QueryResult> void printResults(Tuple2<Time, Time> tt, Query<T> q) {
        System.out.println("========================== " + q.getClass().getSimpleName() + " =======================");
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

    public static void saveToCSV(List<EvalResult> evalResults) {
        // set correct directory as output
        File csvOutputFile = new File(EVALUATION_FILE);
        try (BufferedWriter b = new BufferedWriter(new FileWriter(csvOutputFile))) {
            if (!csvOutputFile.exists()) Files.createFile(csvOutputFile.toPath());
            b.write(EvalResult.toCSVHeader());
            for (EvalResult evalResult : evalResults) {
                b.write(evalResult.toCSV());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println("Times saved to csv file under path: " + csvOutputFile);
    }
}
