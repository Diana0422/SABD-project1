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
import lombok.var;
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
        // overwrites the file
        writer.write(RunResult.toCSVHeader());
    }


    public static void main(String[] args) throws IOException, InstantiationException, IllegalAccessException {
        Evaluation e;
//        e = new Evaluation("-nifi&spark", 10);
//        nifiAndSparkPerformance(e);
        e = new Evaluation("-spark", 10);
        sparkPerformance(e);
    }

    private static void nifiAndSparkPerformance(Evaluation e) throws IOException, InstantiationException, IllegalAccessException {

        List<Class<? extends Query<? extends QueryResult>>> queriesClasses = Arrays.asList(
                Query1.class,
                Query2.class,
                Query3.class,
                QuerySQL1.class,
                QuerySQL2.class,
                QuerySQL3.class);
        List<EvalResult> evalResults = new ArrayList<>();
        for (Class<? extends Query<? extends QueryResult>> queryClass : queriesClasses) {
            Query<? extends QueryResult> q = queryClass.newInstance();
            q.setForcePreprocessing(true);
            Tuple2<Time, Time> eval1 = e.evaluateAll(q, false);
            e.printResults(eval1, q);
            evalResults.add(new EvalResult(queryClass.getSimpleName(), eval1._1, eval1._2));
            System.out.println("======================================================================");
            q.closeSession();
        }
        e.saveToCSV(evalResults);
        // And finally close the sessions
        e.writer.close();
    }

    private static void sparkPerformance(Evaluation e) throws IOException, InstantiationException, IllegalAccessException {
        List<Class<? extends Query<? extends QueryResult>>> queriesClasses = Arrays.asList(
                Query1.class,
                Query2.class,
                Query3.class,
                QuerySQL1.class,
                QuerySQL2.class,
                QuerySQL3.class);
        List<EvalResult> evalResults = new ArrayList<>();
        for (Class<? extends Query<? extends QueryResult>> queryClass : queriesClasses) {
            Query<? extends QueryResult> q = queryClass.newInstance();
            q.setForcePreprocessing(false);
            Tuple2<Time, Time> eval1 = e.evaluateProcessing(q, true);
            e.printResults(eval1, q);
            evalResults.add(new EvalResult(queryClass.getSimpleName(), eval1._1, eval1._2));
            System.out.println("======================================================================");
        }
        e.saveToCSV(evalResults);
        e.writer.close();
    }

    public <T extends QueryResult> Tuple2<Time, Time> evaluateProcessing(Query<T> q, boolean forceRecreateSession) throws InstantiationException, IllegalAccessException {
        q.preProcessing(); // does the processing only if necessary
        List<Time> timesQuery1 = new ArrayList<>();
        for (int i = 0; i < runs; i++) {
            if(forceRecreateSession) {
                q = q.getClass().newInstance();
                q.setForcePreprocessing(false);
            }
            Time time = evaluate(q, i);
            timesQuery1.add(time);
            if(forceRecreateSession) q.closeSession();
        }
        // Don't close now the session or nothing will work!
        return calculateMeanStdev(timesQuery1);
    }

    public <T extends QueryResult> Tuple2<Time, Time> evaluateAll(Query<T> q, boolean forceRecreateSession) throws InstantiationException, IllegalAccessException {
        boolean forcePreprocessing = q.isForcePreprocessing();
        List<Time> timesQuery1 = new ArrayList<>();
        for (int i = 0; i < runs; i++) {
            if(forceRecreateSession) {
                q = q.getClass().newInstance();
                q.setForcePreprocessing(forcePreprocessing);
            }
            Time time = evaluate(q, i);
            timesQuery1.add(time);
            if(forceRecreateSession) q.closeSession();
        }

        // Don't close now the session or nothing will work!
        return calculateMeanStdev(timesQuery1);
    }

    /**
     * Evaluates time for a single query
     *
     * @param <T>                Query1Result, Query2Result,...
     * @param q                  is a query
     * @param runNumber          the index of the run
     * @return the time for the query
     */
    private <T extends QueryResult> Time evaluate(Query<T> q, int runNumber) {
        Time time;
        if (q.isForcePreprocessing()) {
            time = Performance.measureTime(() -> {
                q.preProcessing();
                q.processing();
            });
        } else {
            time = Performance.measureTime(q::processing);
        }

        System.out.println("Run " + (runNumber + 1) + "/" + runs + " time: " + time);
        addToCsv(new RunResult(runNumber + 1, q.getClass().getSimpleName(), time));
        return time;
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
