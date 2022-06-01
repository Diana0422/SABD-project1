package com.sparkling_taxi.bean.query2;

public class Struct1Hour {
    private final int hour;
    private final double tripCount;
    private final double tipAmount;
    private final double squareTipAmount;
    private final long paymentType;

    Struct1Hour(int hour, double tripCount, double tipAmount, double squareTipAmount, long paymentType) {
        this.hour = hour;
        this.tripCount = tripCount;
        this.tipAmount = tipAmount;
        this.squareTipAmount = squareTipAmount;
        this.paymentType = paymentType;
    }

    public int getHour() {
        return hour;
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

    public double getTripCount() {
        return tripCount;
    }

    @Override
    public String toString() {
        return this.hour + ": " + this.tripCount + ", " + this.tipAmount + ", " + this.squareTipAmount + ", " + this.paymentType;
    }
}
