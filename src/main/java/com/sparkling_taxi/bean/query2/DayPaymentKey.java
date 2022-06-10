package com.sparkling_taxi.bean.query2;

import lombok.Data;

import java.io.Serializable;

@Data
public class DayPaymentKey implements Serializable {

    private final String hourDay;
    private final Long paymentType;

    public DayPaymentKey(String hourDay, Long paymentType) {
        this.hourDay = hourDay;
        this.paymentType = paymentType;
    }

}
