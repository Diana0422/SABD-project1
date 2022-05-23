package com.sparkling_taxi;

import java.io.Serializable;
import java.util.Objects;

public class TripleKey implements Serializable {
    private final Integer hourStart;
    private final Integer hourEnd;

    private final Long paymentType;

    public TripleKey(Integer keyA, Integer  keyB, Long paymentType) {
        this.hourStart = keyA;
        this.hourEnd = keyB;
        this.paymentType = paymentType;
    }

    public Integer getHourStart() {
        return hourStart;
    }

    public Integer getHourEnd() {
        return hourEnd;
    }

    public Long getPaymentType() {
        return paymentType;
    }

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
