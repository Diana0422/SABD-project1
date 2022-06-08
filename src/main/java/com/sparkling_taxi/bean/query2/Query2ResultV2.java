package com.sparkling_taxi.bean.query2;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Map;

public class Query2ResultV2 implements Serializable {
    private Double avgTip;
    private Double stdDevTip;
    private Double[] locationDistribution;

    public Query2ResultV2(Query2CalcV2 x) {
        this.avgTip = x.computeMeanTipAmount();
        this.stdDevTip = x.computeStdevTipAmount();
        this.locationDistribution = x.computeLocationDistribution();
    }

    @Override
    public String toString() {
        return "Query2ResultV2{" +
                "avgTip=" + avgTip +
                ", stdDevTip=" + stdDevTip +
                ", locationDistribution=" + Arrays.toString(locationDistribution) +
                '}';
    }
}
