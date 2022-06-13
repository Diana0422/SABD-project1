package com.sparkling_taxi.bean.query3.sql;

import com.sparkling_taxi.bean.QueryResult;
import lombok.Data;

import java.text.DecimalFormat;

@Data
public class CSVQuery3SQL implements QueryResult {
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

    public CSVQuery3SQL(String day,
                        String location1,
                        String location2,
                        String location3,
                        String location4,
                        String location5,
                        String trips1,
                        String trips2,
                        String trips3,
                        String trips4,
                        String trips5,
                        Double meanPassengers1,
                        Double meanPassengers2,
                        Double meanPassengers3,
                        Double meanPassengers4,
                        Double meanPassengers5,
                        String meanFareAmount1,
                        String meanFareAmount2,
                        String meanFareAmount3,
                        String meanFareAmount4,
                        String meanFareAmount5,
                        String stDevFareAmount1,
                        String stDevFareAmount2,
                        String stDevFareAmount3,
                        String stDevFareAmount4,
                        String stDevFareAmount5) {
        this.day = day;
        this.location1 = location1;
        this.location2 = location2;
        this.location3 = location3;
        this.location4 = location4;
        this.location5 = location5;
        this.trips1 = trips1;
        this.trips2 = trips2;
        this.trips3 = trips3;
        this.trips4 = trips4;
        this.trips5 = trips5;
        this.meanPassengers1 = DECIMAL_FORMAT.format(meanPassengers1);
        this.meanPassengers2 = DECIMAL_FORMAT.format(meanPassengers2);
        this.meanPassengers3 = DECIMAL_FORMAT.format(meanPassengers3);
        this.meanPassengers4 = DECIMAL_FORMAT.format(meanPassengers4);
        this.meanPassengers5 = DECIMAL_FORMAT.format(meanPassengers5);
        this.meanFareAmount1 = DECIMAL_FORMAT.format(meanFareAmount1);
        this.meanFareAmount2 = DECIMAL_FORMAT.format(meanFareAmount2);
        this.meanFareAmount3 = DECIMAL_FORMAT.format(meanFareAmount3);
        this.meanFareAmount4 = DECIMAL_FORMAT.format(meanFareAmount4);
        this.meanFareAmount5 = DECIMAL_FORMAT.format(meanFareAmount5);
        this.stDevFareAmount1 = DECIMAL_FORMAT.format(stDevFareAmount1);
        this.stDevFareAmount2 = DECIMAL_FORMAT.format(stDevFareAmount2);
        this.stDevFareAmount3 = DECIMAL_FORMAT.format(stDevFareAmount3);
        this.stDevFareAmount4 = DECIMAL_FORMAT.format(stDevFareAmount4);
        this.stDevFareAmount5 = DECIMAL_FORMAT.format(stDevFareAmount5);
    }

    public CSVQuery3SQL() {
    }
}
