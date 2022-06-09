package com.sparkling_taxi.bean.query1;

import com.sparkling_taxi.bean.QueryResult;
import lombok.Data;

import java.io.Serializable;
import java.text.DecimalFormat;

@Data
public class CSVQuery1 implements QueryResult {

    private static final DecimalFormat DECIMAL_FORMAT = new DecimalFormat("#.####");
    private int year;
    private String month;
    private String avgRatio;
    private long count;

    public CSVQuery1() {
    }

    public CSVQuery1(Query1Result q1) {
        this.year = q1.getYearMonth().getYear();
        this.month = String.format("%02d", q1.getYearMonth().getMonth());
        this.avgRatio = DECIMAL_FORMAT.format(q1.getAvgRatio());
        this.count = (long) q1.getCount();
    }

    public String getYearMonth() {
        return this.year + "/" + String.format("%02d", Integer.parseInt(this.month));
    }
}
