package com.sparkling_taxi.bean.query2;

import com.sparkling_taxi.utils.Utils;

import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;

import static com.sparkling_taxi.utils.Const.*;

public class Query2Calc implements Serializable {
    private Double count;
    private Double tipAmount;
    private final Map<Long, Long> paymentTypeDistribution; // Map(paymentType, numero_occorrenze)
    private final Map<Long, Long> locationDistribution;
    private Double squareTipAmount;

    public Query2Calc(double count, Query2Bean q2b) {
        this.count = count;
        this.tipAmount = q2b.getTip_amount();
        this.squareTipAmount = tipAmount * tipAmount;
        this.paymentTypeDistribution = new HashMap<>();
        this.locationDistribution = new HashMap<>();
        for (Long i = 1L; i <= NUM_PAYMENT_TYPES; i++) {
            this.paymentTypeDistribution.put(i, 0L);
        }
        for (Long i = 1L; i <= NUM_LOCATIONS; i++) {
            this.locationDistribution.put(i, 0L);
        }

        this.paymentTypeDistribution.put(q2b.getPayment_type(), 1L);
        this.locationDistribution.put(q2b.getPULocationID(), 1L);
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
        for (Map.Entry<Long, Long> pt : this.paymentTypeDistribution.entrySet()) {
            Long key = pt.getKey();
            Long type1 = this.paymentTypeDistribution.get(key);
            Long type2 = other.paymentTypeDistribution.get(key);
            this.paymentTypeDistribution.put(key, type1 + type2);
        }
        for (Map.Entry<Long, Long> locs : this.locationDistribution.entrySet()) {
            Long key = locs.getKey();
            Long loc1 = this.locationDistribution.get(key);
            Long loc2 = other.locationDistribution.get(key);
            this.locationDistribution.put(key, loc1 + loc2);
        }
        return this;
    }

    public Double computeMeanTipAmount() {
        return tipAmount / count;
    }

    public Double computeStdevTipAmount() {
        return Utils.stddev(count, tipAmount, squareTipAmount);
    }

    public Long getMostPopularPaymentType() {
        // gets the max occurrence of payment type in the hashmap
        Long max = paymentTypeDistribution.values().stream().mapToLong(l -> l).max().orElse(0L);
        // gets the payment type with max occurences. If nothing is found, returns the UNKNOWN type.
        return paymentTypeDistribution.entrySet()
                .stream()
                .filter(entry -> Objects.equals(entry.getValue(), max))
                .findFirst()
                .map(Map.Entry::getKey)
                .orElse(UNKNOWN_PAYMENT_TYPE);
    }

    public Map<Long, Double> computeLocationDistribution() {
        Map<Long, Double> percent = new HashMap<>();
        for (Map.Entry<Long, Long> locs : this.locationDistribution.entrySet()) {
            Long key = locs.getKey();
            Long loc = this.locationDistribution.get(key);
            percent.put(key, loc/count);
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
