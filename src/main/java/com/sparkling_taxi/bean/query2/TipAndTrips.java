package com.sparkling_taxi.bean.query2;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.io.Serializable;

@Data
@AllArgsConstructor
public class TipAndTrips implements Serializable {
    protected double tripCount;
    protected double tipAmount;

    protected double squareTipAmount;

    public TipTripsAndPayment toTipTripsAndPayment(Long payment){
        return new TipTripsAndPayment(this, payment);
    }

    /**
     * Sums values of this instance with values of another instance,
     * then returns this instance with summed values instead of creating a new one.
     *
     * Used in reduceByKey.
     * @param other another instance of this class
     * @return this instance with summed values.
     */
    public TipAndTrips sumWith(TipAndTrips other){
        this.tripCount += other.tripCount;
        this.tipAmount += other.tipAmount;
        this.squareTipAmount += other.squareTipAmount;
        return this;
    }


    @Override
    public String toString() {
        return "TipAndTrips{" +
               "tripCount=" + tripCount +
               ", tipAmount=" + tipAmount +
               ", squareTipAmount=" + squareTipAmount +
               '}';
    }
}
