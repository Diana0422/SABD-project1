package com.sparkling_taxi.bean.query1;

import com.sparkling_taxi.utils.Utils;
import lombok.Data;
import org.jetbrains.annotations.NotNull;

import java.io.Serializable;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.Objects;

@Data
public class YearMonth implements Serializable, Comparable<YearMonth> {
    private int year;
    private int month;

    public YearMonth() {}

    public YearMonth(Timestamp ts){
        LocalDateTime ld = Utils.toLocalDateTime(ts);
        this.year = ld.getYear();
        this.month = ld.getMonthValue();
    }

    public int compareTo(@NotNull YearMonth o) {
        // the smallest year first
        if (this.year < o.year) {
            return -1; // this yearmonth is smaller than the other
        } else if (o.year == this.year) { // if the two yearMonth have the same year, compare the months
            return Integer.compare(this.month, o.month);
        } else return 1; // the other yearmonth is smaller than this one
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        YearMonth yearMonth = (YearMonth) o;
        return year == yearMonth.year && month == yearMonth.month;
    }

    @Override
    public int hashCode() {
        return Objects.hash(year, month);
    }

    @Override
    public String toString() {
        return String.format("%d/%02d", year, month);
    }
}
