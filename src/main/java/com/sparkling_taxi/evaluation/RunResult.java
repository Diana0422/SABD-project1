package com.sparkling_taxi.evaluation;

import lombok.Data;

@Data
public class RunResult {
    private final int run;
    private final String queryName;
    private final Time average;
    public RunResult(int run, String queryName1, Time average1) {
        this.run = run;
        this.queryName = queryName1;
        this.average = average1;
    }

    public static String toCSVHeader(){
        return "queryName,run,average\n";
    }

    public String toCSV() {
        return this.queryName + "," + run + "," + this.average.toMillis()+"\n";
    }
}
