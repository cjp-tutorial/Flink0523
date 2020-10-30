package com.atguigu.bean;

import lombok.Data;

/**
 * TODO
 *
 * @author cjp
 * @version 1.0
 * @date 2020/10/30 9:20
 */
@Data
public class AdClickLog {
    /**
     * 用户ID
     */
    private Long userId;

    /**
     * 广告ID
     */
    private Long adId;

    /**
     * 省份
     */
    private String province;

    /**
     * 城市
     */
    private String city;

    /**
     * 时间戳
     */
    private Long timestamp;

    public AdClickLog(Long userId, Long adId, String province, String city, Long timestamp) {
        this.userId = userId;
        this.adId = adId;
        this.province = province;
        this.city = city;
        this.timestamp = timestamp;
    }
}
