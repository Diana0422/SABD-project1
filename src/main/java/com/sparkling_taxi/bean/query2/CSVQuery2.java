package com.sparkling_taxi.bean.query2;

import com.sparkling_taxi.bean.query1.Query1Result;
import com.sparkling_taxi.bean.query1.YearMonth;
import lombok.Data;

import java.io.Serializable;
import java.text.DecimalFormat;

@Data
public class CSVQuery2 implements Serializable {

    private static final DecimalFormat DECIMAL_FORMAT = new DecimalFormat("#.####");
    private int year;
    private String month;
    private String avgPassengers;
    private String avgRatio;

    public CSVQuery2() {}
}
