package com.sparkling_taxi.bean.query1;

import lombok.Data;

import java.io.Serializable;
import java.sql.Timestamp;

@Data
public class Query1Bean implements Serializable {
    private Timestamp tpep_dropoff_datetime;
    private double tip_amount;
    private double tolls_amount;
    private double total_amount;

    public Query1Bean(){}
}
