package com.sparkling_taxi.bean.query1;

import com.sparkling_taxi.bean.QueryResult;
import com.sparkling_taxi.bean.query1.Query1Calc;
import lombok.Data;
import scala.Tuple2;

import java.io.Serializable;

@Data
public class Query1Result implements QueryResult {
    private YearMonthKey yearMonth;
    private double avgRatio;
    private double count;

    public Query1Result() {}

    public Query1Result(Tuple2<YearMonthKey, Query1Calc> t) {
        this.yearMonth = t._1;
        this.avgRatio = t._2.computeRatioMean();
        this.count = t._2.getCount();
    }
}
