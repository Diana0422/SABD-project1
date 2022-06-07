package com.sparkling_taxi.bean.query3;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class Zone {
    private String id;
    private String borough;
    private String zone;

    public String zoneString() {
        return borough + " - " + zone;
    }
}
