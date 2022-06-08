package com.sparkling_taxi.bean.query2;

import lombok.Data;

import java.io.Serializable;
import java.util.Map;

@Data
public class Query2Result implements Serializable {
    private Double avgTip;
    private Double stdDevTip;
    private Long popPayment;
    private Map<Long, Double> locationDistribution;

    public Query2Result(Query2Calc x) {
        this.avgTip = x.computeMeanTipAmount();
        this.stdDevTip = x.computeStdevTipAmount();
        this.popPayment = x.getMostPopularPaymentType();
        this.locationDistribution = x.computeLocationDistribution();
    }

    @Override
    public String toString() {
        return "Query2Result{" +
                "avgTip=" + avgTip +
                ", stdDevTip=" + stdDevTip +
                ", popPayment=" + popPayment;
    }
}
