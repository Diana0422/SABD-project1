package com.sparkling_taxi.bean.query3;

import com.sparkling_taxi.utils.Utils;
import lombok.AllArgsConstructor;
import lombok.Data;

import java.io.Serializable;
import java.sql.Timestamp;
import java.util.Objects;

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

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DayLocationKey that = (DayLocationKey) o;
        return Objects.equals(day, that.day) && Objects.equals(destination, that.destination);
    }

    @Override
    public int hashCode() {
        return Objects.hash(day, destination);
    }
}
