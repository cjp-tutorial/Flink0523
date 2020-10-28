package com.atguigu.bean;

import lombok.Data;

/**
 * TODO
 *
 * @author cjp
 * @version 1.0
 * @date 2020/10/28 15:30
 */
@Data
public class UserBehavior {
    private Long userId;
    private Long itemId;
    private Integer categoryId;
    private String behavior;
    private Long timestamp;

    public UserBehavior(Long userId, Long itemId, Integer categoryId, String behavior, Long timestamp) {
        this.userId = userId;
        this.itemId = itemId;
        this.categoryId = categoryId;
        this.behavior = behavior;
        this.timestamp = timestamp;
    }
}
