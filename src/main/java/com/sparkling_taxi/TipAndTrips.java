package com.sparkling_taxi;

public class TipAndTrips {
    private final double tripCount;
    private final double tipAmount;

    private final double squareTipAmount;

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

    public double getTripCount() {
        return tripCount;
    }

    public double getTipAmount() {
        return tipAmount;
    }

    public double getSquareTipAmount() {
        return squareTipAmount;
    }
}
