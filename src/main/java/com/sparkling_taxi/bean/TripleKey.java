package com.sparkling_taxi.bean;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.io.Serializable;
import java.util.Objects;

@Data
@AllArgsConstructor
public class TripleKey implements Serializable {
    private final Integer hourStart;
    private final Integer hourEnd;

    private final Long paymentType;

    @Override
    public String toString() {
        return "TripleKey{" +
               "hourStart=" + hourStart +
               ", hourEnd=" + hourEnd +
               ", paymentType=" + paymentType +
               '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TripleKey tripleKey = (TripleKey) o;
        return Objects.equals(hourStart, tripleKey.hourStart) && Objects.equals(hourEnd, tripleKey.hourEnd) && Objects.equals(paymentType, tripleKey.paymentType);
    }

    @Override
    public int hashCode() {
        return Objects.hash(hourStart, hourEnd, paymentType);
    }
}
