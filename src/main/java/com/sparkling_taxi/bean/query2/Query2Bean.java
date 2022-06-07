package com.sparkling_taxi.bean.query2;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.sql.Timestamp;
@Data
@NoArgsConstructor
@AllArgsConstructor
public class Query2Bean implements Serializable {
    private Timestamp tpep_pickup_datetime;
    private Long PULocationID;
    private Double tip_amount;
    private Long payment_type;
}
