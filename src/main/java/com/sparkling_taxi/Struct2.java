package com.sparkling_taxi;

public class Struct2 {
    private final int hourStart;
    private final int hourEnd;
    private final double tripCount;
    private final double tipAmount;
    private final double squareTipAmount;
    Struct2(int hourStart, int hourEnd,  double tripCount, double tipAmount, double squareTipAmount){
        this.hourStart = hourStart;
        this.hourEnd = hourEnd;
        this.tripCount = tripCount;
        this.tipAmount = tipAmount;
        this.squareTipAmount = squareTipAmount;
    }

    public int getHourStart() {
        return hourStart;
    }

    public int getHourEnd() {
        return hourEnd;
    }

    public double getTipAmount() {
        return tipAmount;
    }

    public double getSquareTipAmount() {
        return squareTipAmount;
    }

    public double getTripCount() {
        return tripCount;
    }
}
