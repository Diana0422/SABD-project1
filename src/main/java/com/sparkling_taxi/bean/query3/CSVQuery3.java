package com.sparkling_taxi.bean.query3;

import lombok.Data;

import java.io.Serializable;
import java.text.DecimalFormat;
import java.util.List;

@Data
public class CSVQuery3 implements Serializable {

    private static final DecimalFormat DECIMAL_FORMAT = new DecimalFormat("#.####");
    private String day;
    private String location1;
    private String location2;
    private String location3;
    private String location4;
    private String location5;
    private String trips1;
    private String trips2;
    private String trips3;
    private String trips4;
    private String trips5;
    private String meanPassengers1;
    private String meanPassengers2;
    private String meanPassengers3;
    private String meanPassengers4;
    private String meanPassengers5;
    private String meanFareAmount1;
    private String meanFareAmount2;
    private String meanFareAmount3;
    private String meanFareAmount4;
    private String meanFareAmount5;
    private String stDevFareAmount1;
    private String stDevFareAmount2;
    private String stDevFareAmount3;
    private String stDevFareAmount4;
    private String stDevFareAmount5;

    public CSVQuery3(String day, List<String> locations, List<String> trips, List<Double> meanPassengers, List<Double> meanFareAmounts, List<Double> stdDevFareAmounts) {
        this.day = day;
        this.location1 = locations.get(0);
        this.location2 = locations.get(1);
        this.location3 = locations.get(2);
        this.location4 = locations.get(3);
        this.location5 = locations.get(4);
        this.trips1 = trips.get(0);
        this.trips2 = trips.get(1);
        this.trips3 = trips.get(2);
        this.trips4 = trips.get(3);
        this.trips5 = trips.get(4);
        this.meanPassengers1 = DECIMAL_FORMAT.format(meanPassengers.get(0));
        this.meanPassengers2 = DECIMAL_FORMAT.format(meanPassengers.get(1));
        this.meanPassengers3 = DECIMAL_FORMAT.format(meanPassengers.get(2));
        this.meanPassengers4 = DECIMAL_FORMAT.format(meanPassengers.get(3));
        this.meanPassengers5 = DECIMAL_FORMAT.format(meanPassengers.get(4));
        this.meanFareAmount1 = DECIMAL_FORMAT.format(meanFareAmounts.get(0));
        this.meanFareAmount2 = DECIMAL_FORMAT.format(meanFareAmounts.get(1));
        this.meanFareAmount3 = DECIMAL_FORMAT.format(meanFareAmounts.get(2));
        this.meanFareAmount4 = DECIMAL_FORMAT.format(meanFareAmounts.get(3));
        this.meanFareAmount5 = DECIMAL_FORMAT.format(meanFareAmounts.get(4));
        this.stDevFareAmount1 = DECIMAL_FORMAT.format(stdDevFareAmounts.get(0));
        this.stDevFareAmount2 = DECIMAL_FORMAT.format(stdDevFareAmounts.get(1));
        this.stDevFareAmount3 = DECIMAL_FORMAT.format(stdDevFareAmounts.get(2));
        this.stDevFareAmount4 = DECIMAL_FORMAT.format(stdDevFareAmounts.get(3));
        this.stDevFareAmount5 = DECIMAL_FORMAT.format(stdDevFareAmounts.get(4));
    }

    public CSVQuery3() {
    }
}
