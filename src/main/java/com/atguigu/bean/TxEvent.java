package com.atguigu.bean;

import lombok.Data;

/**
 * TODO
 *
 * @author cjp
 * @version 1.0
 * @date 2020/10/30 10:23
 */
@Data
public class TxEvent {
    private String txId;
    private String payChannel;
    private Long eventTime;

    public TxEvent(String txId, String payChannel, Long eventTime) {
        this.txId = txId;
        this.payChannel = payChannel;
        this.eventTime = eventTime;
    }
}
