package com.sparkling_taxi.bean.query3;

import lombok.Data;

import java.io.Serializable;
import java.sql.Timestamp;

@Data
public class Query3Bean implements Serializable {
    private Timestamp tpep_pickup_datetime;
    private Timestamp tpep_dropoff_datetime;
    private double passenger_count;
    private Long DOLocationID;
    private double payment_type;
    private double fare_amount;

    public Query3Bean(){}
}
