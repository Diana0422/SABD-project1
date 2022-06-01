package com.sparkling_taxi.bean.query3;

import lombok.Data;

import java.io.Serializable;
import java.text.DecimalFormat;

@Data
public class CSVQuery3 implements Serializable {

    private static final DecimalFormat DECIMAL_FORMAT = new DecimalFormat("#.####");
    private int rank;
    private int trips;
    private String location;
    private String meanPassengers;
    private String meanFareAmount;
    private String stDevFareAmount;

    public CSVQuery3(int rank, Query3Result q3r) {
        this.rank = rank;
        this.trips = q3r.getTrips().intValue();
        this.location = q3r.getLocation().toString();
        this.meanPassengers = DECIMAL_FORMAT.format(q3r.getMeanPassengers());
        this.meanFareAmount = DECIMAL_FORMAT.format(q3r.getMeanFareAmount());
        this.stDevFareAmount = DECIMAL_FORMAT.format(q3r.getStDevFareAmount());
    }

    public CSVQuery3() {}
}
