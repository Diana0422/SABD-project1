package com.sparkling_taxi.evaluation;

import com.sparkling_taxi.bean.QueryResult;
import com.sparkling_taxi.spark.Query;
import com.sparkling_taxi.spark.Query1;
import com.sparkling_taxi.spark.Query2;
import com.sparkling_taxi.spark.Query3;
import com.sparkling_taxi.sparksql.QuerySQL1;
import com.sparkling_taxi.sparksql.QuerySQL2;
import com.sparkling_taxi.sparksql.QuerySQL3;
import com.sparkling_taxi.utils.FileUtils;
import com.sparkling_taxi.utils.Utils;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static com.sparkling_taxi.utils.Const.*;

public class Evaluation {
    private final File csvDetailedOutputFile;
    private final File csvGlobalOutputFile;
    private final BufferedWriter writer;
    private final int runs;

    public Evaluation(String fileInputPrefix, int runs) throws IOException {
        this.csvDetailedOutputFile = new File(FileUtils.appendEndOfFileName(DETAILED_EVALUATION_FILE, fileInputPrefix));
        this.csvGlobalOutputFile = new File(FileUtils.appendEndOfFileName(GLOBAL_EVALUATION_FILE, fileInputPrefix));
        this.writer = new BufferedWriter(new FileWriter(csvDetailedOutputFile));
        this.runs = runs;
    }


    public static void main(String[] args) throws IOException {
        Evaluation e;
        e = new Evaluation("-nifi&spark", 10);
        nifiAndSparkPerformance(e);
        e = new Evaluation("-spark", 40);
        sparkPerformance(e);
    }

    private static void nifiAndSparkPerformance(Evaluation e) throws IOException {
        SparkSession s = SparkSession
                .builder()
                .master("spark://spark:7077")
                .appName("Evaluation")
                .getOrCreate();
        List<Query<? extends QueryResult>> queries = Arrays.asList(new Query1(true, s), new Query2(true, s), new Query3(true, s), new QuerySQL1(true, s), new QuerySQL2(true, s), new QuerySQL3(true, s));
        List<EvalResult> evalResults = new ArrayList<>();
        for (Query<? extends QueryResult> query : queries) {
            Tuple2<Time, Time> eval1 = e.evaluate(query);
            e.printResults(eval1, query);
            evalResults.add(new EvalResult(query.getClass().getSimpleName(), eval1._1, eval1._2));
            System.out.println("======================================================================");
        }
        e.saveToCSV(evalResults);
        // And finally close the sessions
        queries.forEach(Query::closeSession);
        e.writer.close();
    }

    private static void sparkPerformance(Evaluation e) throws IOException {
        SparkSession s = SparkSession
                .builder()
                .master("spark://spark:7077")
                .appName("Evaluation")
                .getOrCreate();
        List<Query<? extends QueryResult>> queries = Arrays.asList(new Query1(s), new Query2(s), new Query3(s), new QuerySQL1(s), new QuerySQL2(s), new QuerySQL3(s));
        List<EvalResult> evalResults = new ArrayList<>();
        for (Query<? extends QueryResult> query : queries) {
            Tuple2<Time, Time> eval1 = e.evaluate(query);
            e.printResults(eval1, query);
            evalResults.add(new EvalResult(query.getClass().getSimpleName(), eval1._1, eval1._2));
            System.out.println("======================================================================");
        }
        e.saveToCSV(evalResults);
        // And finally close the sessions
        queries.forEach(Query::closeSession);
        e.writer.close();
    }

    public <T extends QueryResult> Tuple2<Time, Time> evaluate(Query<T> q) {
        if (!q.isForcePreprocessing()){
            q.preProcessing();
        }
        List<Time> timesQuery1 = new ArrayList<>();
        for (int i = 0; i < runs; i++) {
            Time time;
            if (q.isForcePreprocessing()){
                time = Performance.measureTime(() -> {
                    q.preProcessing();
                    q.processing();
                });
            } else {
                time = Performance.measureTime(q::processing);
            }
            timesQuery1.add(time);
            System.out.println("Run " + (i + 1) + "/" + runs + " time: " + time);
            addToCsv(new RunResult(i + 1, q.getClass().getSimpleName(), time));
        }
        // Don't close now the session or nothing will work!
        return calculateMeanStdev(timesQuery1);
    }

    public <T extends QueryResult> void printResults(Tuple2<Time, Time> tt, Query<T> q) {
        System.out.println("========================== " + q.getClass().getSimpleName() + " =======================");
        System.out.println("Number of Runs: " + runs + " Average Time: " + tt._1 + " Standard Deviation: " + tt._2);
    }

    public Tuple2<Time, Time> calculateMeanStdev(List<Time> t) {
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

    public void saveToCSV(List<EvalResult> evalResults) {
        // set correct directory as output
        try (BufferedWriter b = new BufferedWriter(new FileWriter(csvGlobalOutputFile))) {
            if (!csvGlobalOutputFile.exists()) Files.createFile(csvGlobalOutputFile.toPath());
            b.write(EvalResult.toCSVHeader());
            for (EvalResult evalResult : evalResults) {
                b.write(evalResult.toCSV());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println("Times saved to csv file under path: " + csvGlobalOutputFile);
    }

    public void addToCsv(RunResult runResult) {
        try {
            if (!csvDetailedOutputFile.exists()) {
                Files.createFile(csvDetailedOutputFile.toPath());
                writer.append(RunResult.toCSVHeader());
            }
            writer.append(runResult.toCSV());
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println("Times saved to csv file under path: " + csvDetailedOutputFile);
    }

    public void closeWriter() throws IOException {
        writer.close();
    }
}
