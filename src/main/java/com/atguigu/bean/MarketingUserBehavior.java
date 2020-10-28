package com.atguigu.bean;

import lombok.Data;

/**
 * TODO
 *
 * @author cjp
 * @version 1.0
 * @date 2020/10/28 16:42
 */
@Data
public class MarketingUserBehavior {
    private Long userId;
    private String behavior;
    private String channel;
    private Long timestamp;

    public MarketingUserBehavior(Long userId, String behavior, String channel, Long timestamp) {
        this.userId = userId;
        this.behavior = behavior;
        this.channel = channel;
        this.timestamp = timestamp;
    }
}
