package com.sparkling_taxi.bean.query2;

import com.sparkling_taxi.bean.QueryResult;
import lombok.Data;
import scala.Tuple2;

import java.util.Map;

@Data
public class Query2Result implements QueryResult {
    private final String hour;
    private Double avgTip;
    private Double stdDevTip;
    private Long popPayment;
    private Map<Long, Double> locationDistribution;

    public Query2Result(Tuple2<String, Query2Calc> x) {
        this.hour = x._1;
        this.avgTip = x._2.computeMeanTipAmount();
        this.stdDevTip = x._2.computeStdevTipAmount();
        this.popPayment = x._2.getMostPopularPaymentType();
        this.locationDistribution = x._2.computeLocationDistribution();
    }

    @Override
    public String toString() {
        return "Query2Result{" +
                "avgTip=" + avgTip +
                ", stdDevTip=" + stdDevTip +
                ", popPayment=" + popPayment;
    }
}
