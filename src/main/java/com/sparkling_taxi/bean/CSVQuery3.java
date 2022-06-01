package com.sparkling_taxi.bean;

import lombok.Data;

import java.io.Serializable;
import java.text.DecimalFormat;

@Data
public class CSVQuery3 implements Serializable {

    private static final DecimalFormat DECIMAL_FORMAT = new DecimalFormat("#.####");
    private int rank;
    private int taxiRides;
    private String location;
    private String meanPassengers;
    private String meanFareAmount;
    private String stDevFareAmount;

    public CSVQuery3(int rank, int taxiRides, String location, double meanPassengers, double meanFareAmount, double stDevFareAmount) {
        this.rank = rank;
        this.taxiRides = taxiRides;
        this.location = location;
        this.meanPassengers = DECIMAL_FORMAT.format(meanPassengers);
        this.meanFareAmount = DECIMAL_FORMAT.format(meanFareAmount);
        this.stDevFareAmount = DECIMAL_FORMAT.format(stDevFareAmount);
    }

    public CSVQuery3() {}
}
