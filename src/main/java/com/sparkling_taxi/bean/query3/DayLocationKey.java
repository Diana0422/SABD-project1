package com.sparkling_taxi.bean.query3;

import com.sparkling_taxi.utils.Utils;
import lombok.AllArgsConstructor;
import lombok.Data;

import java.io.Serializable;
import java.sql.Timestamp;

@Data
@AllArgsConstructor
public class DayLocationKey implements Serializable {
    private String day;
    private Long destination;

    public DayLocationKey(){}

    public DayLocationKey(Timestamp tpep_dropoff_datetime, Long doLocationID) {
        this.day = Utils.getDay(tpep_dropoff_datetime);
        this.destination = doLocationID;
    }
}
