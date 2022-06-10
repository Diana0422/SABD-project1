package com.sparkling_taxi.bean.query2;

import lombok.Data;

import java.io.Serializable;

@Data
public class PaymentCount implements Serializable {
    private final Long paymentType;
    private final Integer occurrence;

    public PaymentCount(Long paymentType, Integer count) {
        this.paymentType = paymentType;
        this.occurrence = count;
    }
}
