package com.sparkling_taxi.bean.query2;

import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.text.DecimalFormat;

@Data
@NoArgsConstructor
public class CSVQuery2 implements Serializable {

    private static final DecimalFormat DECIMAL_FORMAT = new DecimalFormat("#.######");
    private String hour;
    private Double avgTip;
    private Double stdDevTip;
    private Long popPayment;
    private String locationDistribution;

    public CSVQuery2(Query2Result query2Result) {
        this.hour = query2Result.getHour();
        this.avgTip = query2Result.getAvgTip();
        this.stdDevTip = query2Result.getStdDevTip();
        this.popPayment = query2Result.getPopPayment();
        Double[] locDistribution = query2Result.getLocationDistribution();
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < locDistribution.length; i++) {
            sb.append(DECIMAL_FORMAT.format(locDistribution[i]));
            if (i != locDistribution.length - 1) {
                sb.append("-");
            }
        }
        this.locationDistribution = sb.toString();
    }
}
