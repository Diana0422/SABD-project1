package com.sparkling_taxi.bean;

import java.io.Serializable;

public class TipAndTrips implements Serializable {
    protected double tripCount;
    protected double tipAmount;

    protected double squareTipAmount;

    public TipAndTrips(double tripCount, double tipAmount, double squareTipAmount) {
        this.tripCount = tripCount;
        this.tipAmount = tipAmount;
        this.squareTipAmount = squareTipAmount;
    }

    public TipAndTrips(Struct1Hour s){
        this.tripCount = s.getTripCount();
        this.tipAmount = s.getTipAmount();
        this.squareTipAmount = s.getSquareTipAmount();
    }

    public TipTripsAndPayment toTipTripsAndPayment(Long payment){
        return new TipTripsAndPayment(this, payment);
    }

    public double getTripCount() {
        return tripCount;
    }

    public double getTipAmount() {
        return tipAmount;
    }

    public double getSquareTipAmount() {
        return squareTipAmount;
    }

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
