package com.sparkling_taxi;

public class Key1 {
    private final Integer hourStart;
    private final Integer hourEnd;

    public Key1(Integer keyA, Integer  keyB) {
        this.hourStart = keyA;
        this.hourEnd = keyB;
    }

    public Integer getHourStart() {
        return hourStart;
    }

    public Integer getHourEnd() {
        return hourEnd;
    }
}
