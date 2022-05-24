package com.sparkling_taxi;

public class TipTripsAndPayment extends TipAndTrips {


    private Long payment;

    public TipTripsAndPayment(TipAndTrips s, Long payment){
        super(s.tripCount, s.tipAmount, s.squareTipAmount);
        this.payment = payment;
    }

    public TipTripsAndPayment sumWith(TipTripsAndPayment other){
        // TODO: watch out forse è sbagliata!!!
        if (this.tripCount < other.tripCount){
            this.payment = other.payment;
        }

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

    public Long getPayment() {
        return payment;
    }
}
