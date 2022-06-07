package com.sparkling_taxi.bean.query3;

import lombok.Data;

import java.io.Serializable;
import java.text.DecimalFormat;
import java.util.Comparator;

@Data
public class Query3Result implements Serializable {
    private static final DecimalFormat DECIMAL_FORMAT = new DecimalFormat("#.####");

//    private DayLocationKey dayLocationKey ;
    private Double trips;
    private Double meanPassengers;
    private Double meanFareAmount;
    private Double stDevFareAmount;

    public Query3Result(Query3Calc q3){
//        this.dayLocationKey = dayLocation;
        this.trips = q3.getCount();
        this.meanPassengers = q3.computePassengerMean();
        this.meanFareAmount = q3.computeFareAmountMean();
        this.stDevFareAmount = q3.computeFareAmountStdev();
    }

    @Override
    public String toString() {
        return "Query3Result{" +
//               "dayLocationKey=" + dayLocationKey +
               ", trips=" + trips +
               ", meanPassengers=" + meanPassengers +
               ", meanFareAmount=" + meanFareAmount +
               ", stDevFareAmount=" + stDevFareAmount +
               '}';
    }

    public static final class Query3Comparator implements Comparator<Query3Result>, Serializable {
        public final static Query3Comparator INSTANCE = new Query3Comparator();

        @Override
        public int compare(Query3Result o1, Query3Result o2) {
            return -o1.getTrips().compareTo(o2.getTrips());
        }
    }

}
