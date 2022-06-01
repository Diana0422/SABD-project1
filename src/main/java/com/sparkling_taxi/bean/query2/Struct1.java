package com.sparkling_taxi.bean.query2;

public final class Struct1 {
    private final int hourStart;
    private final int hourEnd;
    private final int tripCount;
    private final double tipAmount;
    private final double squareTipAmount;
    private final long paymentType;
    Struct1(int hourStart, int hourEnd, int tripCount, double tipAmount, double squareTipAmount, long paymentType){
        this.hourStart = hourStart;
        this.hourEnd = hourEnd;
        this.tripCount = tripCount;
        this.tipAmount = tipAmount;
        this.squareTipAmount = squareTipAmount;
        this.paymentType = paymentType;
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

    public long getPaymentType() {
        return paymentType;
    }

    public int getTripCount() {
        return tripCount;
    }
}