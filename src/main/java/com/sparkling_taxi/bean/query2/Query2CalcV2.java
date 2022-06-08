package com.sparkling_taxi.bean.query2;

import com.sparkling_taxi.utils.Utils;
import lombok.Data;
import java.io.Serializable;
import java.util.Arrays;

import static com.sparkling_taxi.utils.Const.NUM_LOCATIONS;

@Data
public class Query2CalcV2 implements Serializable {
    private Double count;
    private Double tipAmount;
    private Double squareTipAmount;
    private Long[] locationDistribution;

    public Query2CalcV2(double count, Query2Bean q2b) {
        this.count = count;
        this.tipAmount = q2b.getTip_amount();
        this.squareTipAmount = tipAmount * tipAmount;
        this.locationDistribution = new Long[NUM_LOCATIONS.intValue()];
        for (int i = 0; i < NUM_LOCATIONS; i++) {
            this.locationDistribution[i] = 0L;
        }
        this.locationDistribution[q2b.getPULocationID().intValue()-1] = 1L;
    }

    /**
     * Sums values of this instance with values of another instance,
     * then returns this instance with summed values instead of creating a new one.
     * <p>
     * Used in reduceByKey.
     *
     * @param other another instance of this class
     * @return this instance with summed values.
     */
    public Query2CalcV2 sumWith(Query2CalcV2 other) {
        this.count += other.count;
        this.tipAmount += other.tipAmount;
        this.squareTipAmount += other.squareTipAmount;
        for (int i = 0; i < NUM_LOCATIONS; i++) {
            Long loc1 = this.locationDistribution[i];
            Long loc2 = other.locationDistribution[i];
            this.locationDistribution[i] = loc1 + loc2;
        }
        return this;
    }

    @Override
    public String toString() {
        return "Query2CalcV2{" +
                "count=" + count +
                ", tipAmount=" + tipAmount +
                ", squareTipAmount=" + squareTipAmount +
                ", locationDistribution=" + Arrays.toString(locationDistribution) +
                '}';
    }

    public Double computeMeanTipAmount() {
        return tipAmount / count;
    }

    public Double computeStdevTipAmount() {
        return Utils.stddev(count, tipAmount, squareTipAmount);
    }

    public Double[] computeLocationDistribution() {
        Double[] percent = new Double[NUM_LOCATIONS.intValue()];
        for (int i = 0; i < NUM_LOCATIONS; i++) {
            Long loc1 = this.locationDistribution[i];
            percent[i] = loc1 / count;
        }
        return percent;
    }
}
