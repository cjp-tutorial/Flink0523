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
public class AdClickCountByUserWithWindowEnd {
    private Long adId;
    private Long userId;
    private Long adClickCount;
    private Long windowEnd;

    public AdClickCountByUserWithWindowEnd(Long adId, Long userId, Long adClickCount, Long windowEnd) {
        this.adId = adId;
        this.userId = userId;
        this.adClickCount = adClickCount;
        this.windowEnd = windowEnd;
    }
}
