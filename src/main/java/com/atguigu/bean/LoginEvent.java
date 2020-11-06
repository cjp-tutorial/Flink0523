package com.atguigu.bean;

import lombok.Data;

/**
 * TODO
 *
 * @author cjp
 * @version 1.0
 * @date 2020/11/6 9:12
 */
@Data
public class LoginEvent {
    private Long userId;
    private String ip;
    private String eventType;
    private Long eventTime;

    public LoginEvent(Long userId, String ip, String eventType, Long eventTime) {
        this.userId = userId;
        this.ip = ip;
        this.eventType = eventType;
        this.eventTime = eventTime;
    }
}
