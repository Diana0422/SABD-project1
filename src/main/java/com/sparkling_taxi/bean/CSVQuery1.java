package com.sparkling_taxi.bean;

import lombok.Data;

import java.io.Serializable;

@Data
public class CSVQuery1 implements Serializable {
    private int year;
    private int month;
    private double avgPassengers;
    private double avgRatio;

    public CSVQuery1() {}
}
