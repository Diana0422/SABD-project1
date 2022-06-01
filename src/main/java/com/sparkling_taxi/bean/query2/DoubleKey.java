package com.sparkling_taxi.bean.query2;

import lombok.Data;

import java.io.Serializable;
import java.util.Objects;

@Data
public class DoubleKey implements Serializable {
    private final Integer hour;

    private final Long paymentType;

    public DoubleKey(Integer keyA, Long paymentType) {
        this.hour = keyA;
        this.paymentType = paymentType;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DoubleKey doubleKey = (DoubleKey) o;
        return Objects.equals(hour, doubleKey.hour) && Objects.equals(paymentType, doubleKey.paymentType);
    }

    @Override
    public int hashCode() {
        return Objects.hash(hour, paymentType);
    }

    @Override
    public String toString() {
        return "DoubleKey{" +
               "hour=" + hour +
               ", paymentType=" + paymentType +
               '}';
    }
}
