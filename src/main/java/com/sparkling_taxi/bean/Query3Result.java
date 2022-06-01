package com.sparkling_taxi.bean;

import lombok.Data;

import java.io.Serializable;
import java.text.DecimalFormat;
import java.util.Comparator;

@Data
public class Query3Result implements Serializable {
    private static final DecimalFormat DECIMAL_FORMAT = new DecimalFormat("#.####");
    private final Double trips;
    private Long location;
    private Double meanPassengers;
    private Double meanFareAmount;
    private Double stDevFareAmount;

    public Query3Result(Double count, Long location, Double meanPassengers, Double meanFareAmount, Double stDevFareAmount) {
        this.trips = count;
        this.location = location;
        this.meanPassengers = meanPassengers;
        this.meanFareAmount = meanFareAmount;
        this.stDevFareAmount = stDevFareAmount;
    }

    @Override
    public String toString() {
        return String.format("taxi_rides = %g, locationID = %d, mean_passengers = %g, mean_fare_amount = %s $ stdev_fare_amount %s $\n",
                trips, location, meanPassengers, meanFareAmount, stDevFareAmount);
    }

    public static final class Query3Comparator implements Comparator<Query3Result>, Serializable {
        public final static Query3Comparator INSTANCE = new Query3Comparator();

        @Override
        public int compare(Query3Result o1, Query3Result o2) {
            return -o1.getTrips().compareTo(o2.getTrips());
        }
    }

}
