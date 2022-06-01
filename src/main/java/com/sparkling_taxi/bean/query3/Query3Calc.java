package com.sparkling_taxi.bean.query3;

import com.sparkling_taxi.utils.Utils;
import lombok.Data;

import java.io.Serializable;

@Data
public class Query3Calc implements Serializable {
    private double count;
    private double passengers;
    private double fareAmount;
    private double squareFareAmount;

    public Query3Calc(double count, Query3Bean q3) {
        this.count = count;
        this.passengers = q3.getPassenger_count();
        this.fareAmount = q3.getFare_amount();
        this.squareFareAmount = this.fareAmount * this.fareAmount;
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
    public Query3Calc sumWith(Query3Calc other) {
        this.count += other.count;
        this.passengers += other.passengers;
        this.fareAmount += other.fareAmount;
        this.squareFareAmount += other.squareFareAmount;
        return this;
    }
    public Double computePassengerMean() {
        return this.passengers / count;
    }

    public Double computeFareAmountMean() {
        return this.fareAmount / count;
    }

    public Double computeFareAmountStdev() {
        return Utils.stddev(this.count, this.fareAmount, this.squareFareAmount);
    }

    @Override
    public String toString() {
        return "Query3Calc{" +
               "trips=" + count +
               ", passengers=" + passengers +
               ", fareAmount=" + fareAmount +
               ", squareFareAmount=" + squareFareAmount +
               '}';
    }
}
