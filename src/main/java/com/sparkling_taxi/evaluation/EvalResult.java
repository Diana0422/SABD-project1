package com.sparkling_taxi.evaluation;

import lombok.AllArgsConstructor;
import lombok.Data;
@Data
@AllArgsConstructor
public class EvalResult {
    String queryName;
    Time average;
    Time standardDeviation;

    public String toCSV(){
        return this.queryName+","+this.average.toMillis()+","+this.standardDeviation.toMillis()+"\n";
    }

    public static String toCSVHeader(){
        return "queryName,average,standardDeviation\n";
    }
}
