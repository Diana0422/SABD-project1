package com.sparkling_taxi.bean;

import lombok.Data;

import java.io.Serializable;
import java.sql.Timestamp;

@Data
public class Query1Bean implements Serializable {
    private Timestamp tpep_pickup_datetime;
    private Timestamp tpep_dropoff_datetime;
    private double passenger_count;
    private double tip_amount;
    private double tolls_amount;
    private double total_amount;

    public Query1Bean(){}
}
