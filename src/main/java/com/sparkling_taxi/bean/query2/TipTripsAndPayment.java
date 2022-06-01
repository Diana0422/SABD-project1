package com.sparkling_taxi.bean.query2;

public class TipTripsAndPayment extends TipAndTrips {


    private Long payment;

    public TipTripsAndPayment(TipAndTrips s, Long payment){
        super(s.tripCount, s.tipAmount, s.squareTipAmount);
        this.payment = payment;
    }

    /**
     * Sums values of this instance with values of another instance,
     * then returns this instance with summed values instead of creating a new one.
     *
     * Used in reduceByKey.
     * @param other another instance of this class
     * @return this instance with summed values.
     */
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
