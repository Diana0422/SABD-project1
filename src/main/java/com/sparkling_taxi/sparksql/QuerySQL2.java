package com.sparkling_taxi.sparksql;

import com.sparkling_taxi.bean.query2.CSVQuery2;
import com.sparkling_taxi.spark.Query;

import java.util.List;

public class QuerySQL2 extends Query<CSVQuery2> {
    @Override
    public void preProcessing() {

    }

    @Override
    public List<CSVQuery2> processing() {
        System.out.println("======================= Running " + this.getClass().getSimpleName() + "=======================");
        return null;
    }

    @Override
    public void postProcessing(List<CSVQuery2> queryResultList) {

    }
}
