package com.atguigu.bean;

import lombok.Data;

/**
 * TODO
 *
 * @author cjp
 * @version 1.0
 * @date 2020/11/3 14:43
 */
@Data
public class AdClickCountWithWindowEnd {
    private Long adId;
    private String province;
    private Long adClickCount;
    private Long windowEnd;

    public AdClickCountWithWindowEnd(Long adId, String province, Long adClickCount, Long windowEnd) {
        this.adId = adId;
        this.province = province;
        this.adClickCount = adClickCount;
        this.windowEnd = windowEnd;
    }
}
