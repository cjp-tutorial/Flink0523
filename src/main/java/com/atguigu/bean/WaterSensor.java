package com.atguigu.bean;

import lombok.*;

/**
 * TODO
 *
 * @author cjp
 * @version 1.0
 * @date 2020/10/27 14:22
 */
@Data
public class WaterSensor {
    // 传感器id
    private String id;
    // 时间戳
    private Long ts;
    // 空高：为了方便，这里认为是水位
    private Integer vc;

    public WaterSensor() {
    }

    public WaterSensor(String id, Long ts, Integer vc) {
        this.id = id;
        this.ts = ts;
        this.vc = vc;
    }
}
