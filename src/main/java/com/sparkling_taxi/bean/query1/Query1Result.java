package com.sparkling_taxi.bean.query1;

import com.sparkling_taxi.bean.query1.Query1Calc;
import lombok.Data;

import java.io.Serializable;

@Data
public class Query1Result implements Serializable {
    private double avgPassengers;
    private double avgRatio;

    public Query1Result() {}

    public Query1Result(Query1Calc calc){
        this.avgPassengers = calc.computePassengerMean();
        this.avgRatio = calc.computeRatioMean();
    }
}