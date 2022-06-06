package com.sparkling_taxi.bean.query1;

import lombok.Data;

import java.io.Serializable;
import java.text.DecimalFormat;

@Data
public class CSVQuery1 implements Serializable {

    private static final DecimalFormat DECIMAL_FORMAT = new DecimalFormat("#.####");
    private int year;
    private String month;
    private String avgRatio;
    private long count;

    public CSVQuery1() {
    }

    public CSVQuery1(YearMonthKey yearMonth, Query1Result query1Result) {
        this.year = yearMonth.getYear();
        this.month = String.format("%02d", yearMonth.getMonth());
        this.avgRatio = DECIMAL_FORMAT.format(query1Result.getAvgRatio());
        this.count = (long) query1Result.getCount();
    }

    public String getYearMonth() {
        return this.year + "/" + String.format("%02d", Integer.parseInt(this.month));
    }
}
