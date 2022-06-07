package com.sparkling_taxi.bean.query3;

import lombok.Data;

import java.io.Serializable;
import java.text.DecimalFormat;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

@Data
public class Query3Result implements Serializable {
    private static final DecimalFormat DECIMAL_FORMAT = new DecimalFormat("#.####");
    private final Long location;
    private String day;
    private Double trips;
    private Double meanPassengers;
    private Double meanFareAmount;
    private Double stDevFareAmount;

    public Query3Result(String day, Long location, Query3Calc q3){
        this.day = day;
        this.location = location;
        this.trips = q3.getCount();
        this.meanPassengers = q3.computePassengerMean();
        this.meanFareAmount = q3.computeFareAmountMean();
        this.stDevFareAmount = q3.computeFareAmountStdev();
    }

    @Override
    public String toString() {
        return "Query3Result{" +
                "location=" + location +
                ", day='" + day + '\'' +
                ", trips=" + trips +
                ", meanPassengers=" + meanPassengers +
                ", meanFareAmount=" + meanFareAmount +
                ", stDevFareAmount=" + stDevFareAmount +
                '}';
    }

    public List<String> toList() {
        return Arrays.asList(
          this.location.toString(),
          this.trips.toString(),
          this.meanPassengers.toString(),
          this.meanFareAmount.toString(),
          this.stDevFareAmount.toString()
        );
    }

    public static final class Query3Comparator implements Comparator<Query3Result>, Serializable {
        public final static Query3Comparator INSTANCE = new Query3Comparator();

        @Override
        public int compare(Query3Result o1, Query3Result o2) {
            return -o1.getTrips().compareTo(o2.getTrips());
        }
    }

}
