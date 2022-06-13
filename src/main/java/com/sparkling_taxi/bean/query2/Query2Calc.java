package com.sparkling_taxi.bean.query2;

import com.sparkling_taxi.utils.Utils;

import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;

import static com.sparkling_taxi.utils.Const.*;

public class Query2Calc implements Serializable {
    private Double count;
    private Double tipAmount;
    private final Long[] paymentTypeDistribution; // Map(paymentType, numero_occorrenze)
    private final Long[] locationDistribution;
    private Double squareTipAmount;

    public Query2Calc(double count, Query2Bean q2b) {
        this.count = count;
        this.tipAmount = q2b.getTip_amount();
        this.squareTipAmount = tipAmount * tipAmount;
        this.paymentTypeDistribution = new Long[NUM_PAYMENT_TYPES.intValue()];
        this.locationDistribution = new Long[NUM_LOCATIONS.intValue()];
        for (int i = 0; i < NUM_PAYMENT_TYPES; i++) {
            this.paymentTypeDistribution[i] = 0L;
        }
        for (int i = 0; i < NUM_LOCATIONS; i++) {
            this.locationDistribution[i] = 0L;
        }
        System.out.println((q2b.getPayment_type().intValue() - 1));
        this.paymentTypeDistribution[q2b.getPayment_type().intValue()-1] = 1L;
        this.locationDistribution[q2b.getPULocationID().intValue()-1] = 1L;
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
    public Query2Calc sumWith(Query2Calc other) {
        this.count += other.count;
        this.tipAmount += other.tipAmount;
        this.squareTipAmount += other.squareTipAmount;
        for (int i = 0; i < NUM_PAYMENT_TYPES; i++) {
            Long loc1 = this.paymentTypeDistribution[i];
            Long loc2 = other.paymentTypeDistribution[i];
            this.paymentTypeDistribution[i] = loc1 + loc2;
        }
        for (int i = 0; i < NUM_LOCATIONS; i++) {
            Long loc1 = this.locationDistribution[i];
            Long loc2 = other.locationDistribution[i];
            this.locationDistribution[i] = loc1 + loc2;
        }
        return this;
    }

    public Double computeMeanTipAmount() {
        return tipAmount / count;
    }

    public Double computeStdevTipAmount() {
        return Utils.stddev(count, tipAmount, squareTipAmount);
    }

    /**
     * Gets the payment type (numeric value) that corresponds to the highest value
     * in the paymentTypeDistribution array.
     * @return the Long value of the payment type
     */
    public Long getMostPopularPaymentType() {
        // gets the payment type with max occurences. If nothing is found, returns the UNKNOWN type.
        long argmax = 0L;
        Long maxVal = 0L;
        for (int i = 0; i < NUM_PAYMENT_TYPES; i++) {
            Long occ = paymentTypeDistribution[i];
            if (occ > maxVal) {
                argmax = (i + 1);
                maxVal = occ;
            }
        }
        return argmax;
    }

    /**
     * Computes the distribution for each location Long in the array locationDistribution
     * with respect to the total number of trips obtained previously
     * @return array of Double that contains the percentage value of the distribution for each location
     */
    public Double[] computeLocationDistribution() {
        Double[] percent = new Double[NUM_LOCATIONS.intValue()];
        for (int i = 0; i < NUM_LOCATIONS; i++) {
            Long loc1 = this.locationDistribution[i];
            percent[i] = loc1 / count;
        }
        return percent;
    }

    @Override
    public String toString() {
        return "Query2Calc{" +
                "count=" + count +
                ", tipAmount=" + tipAmount +
                ", paymentTypeDistribution=" + paymentTypeDistribution +
                ", locationDistribution=" + locationDistribution +
                ", squareTipAmount=" + squareTipAmount +
                '}';
    }
}
