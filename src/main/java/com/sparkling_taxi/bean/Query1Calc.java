package com.sparkling_taxi.bean;

import lombok.Data;

import java.io.Serializable;
@Data
public class Query1Calc implements Serializable {
    private double count;
    private double passengers;
    private double ratio;

    public Query1Calc(double count, Query1Bean q1) {
        this.count = count;
        this.passengers = q1.getPassenger_count();
        this.ratio = q1.getTip_amount() / (q1.getTotal_amount() - q1.getTolls_amount());
        if (Double.isNaN(this.ratio)){
            this.ratio = 0.0;
        }
    }

    /**
     * Sums values of this instance with values of another instance,
     * then returns this instance with summed values instead of creating a new one.
     * <p>
     * Used in reduceByKey.
     *
     * @param other another instance of this class
     * @return this instance with summed values.
     */
    public Query1Calc sumWith(Query1Calc other) {
        this.count += other.count;
        this.passengers += other.passengers;
        this.ratio += other.ratio;
        return this;
    }
    public Double computePassengerMean() {
        return this.passengers / count;
    }

    public Double computeRatioMean() {
        return this.ratio / count;
    }

    @Override
    public String toString() {
        return "Query1Calc{" +
               "count=" + count +
               ", passengers=" + passengers +
               ", ratio=" + ratio +
               '}';
    }
}
