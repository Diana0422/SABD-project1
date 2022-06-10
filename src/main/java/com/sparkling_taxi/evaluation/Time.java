package com.sparkling_taxi.evaluation;

import lombok.Data;

/**
 * Simple class to store millis, seconds and minutes of a computation
 */
@Data
public class Time {
    private int millis;
    private int seconds;
    private int minutes;

    public Time(int seconds, int millis) {
        this.millis = millis;
        this.seconds = seconds;
    }
    public Time(int minutes, int seconds, int millis) {
        this.millis = millis;
        this.seconds = seconds;
        this.minutes = minutes;
    }

    public Time(long totalMillis){
        if (totalMillis < 1000) {
            this.millis = (int) totalMillis;
            this.seconds = 0;
            this.minutes = 0;
        } else if (totalMillis / 1000 < 60) {
            this.seconds = (int) (totalMillis / 1000);
            this.millis = (int) (totalMillis - seconds * 1000);
            this.minutes = 0;
        } else if (totalMillis / 60000 < 60) {
            this.minutes = (int) (totalMillis / 60000);
            this.seconds = (int) ((totalMillis - minutes * 60000) / 1000);
            this.millis = (int) (totalMillis - minutes * 60000 - seconds * 1000);
        } else {
            System.out.println("More than one hour!");
            throw new IllegalStateException();
        }
    }

    public long toMillis(){
        return millis + seconds * 1000L + minutes * 60000L;
    }

    @Override
    public String toString() {
        if (minutes > 0){
            return String.format("Minutes: %d, Seconds: %d, Millis: %d", minutes, seconds, millis);
        } else if (seconds > 0) {
            return String.format("Seconds: %d, Millis: %d", seconds, millis);
        } else {
            return String.format("Time in millis: %d", millis);
        }
    }
}
