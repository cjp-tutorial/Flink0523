package com.atguigu.bean;

import lombok.Data;

/**
 * TODO
 *
 * @author cjp
 * @version 1.0
 * @date 2020/11/3 10:19
 */
@Data
public class ItemCountWithWindowEnd implements Comparable<ItemCountWithWindowEnd> {
    private Long itemId;
    private Long itemCount;
    private Long windowEnd;

    public ItemCountWithWindowEnd(Long itemId, Long itemCount, Long windowEnd) {
        this.itemId = itemId;
        this.itemCount = itemCount;
        this.windowEnd = windowEnd;
    }

    @Override
    public int compareTo(ItemCountWithWindowEnd o) {
        return o.getItemCount().intValue() - this.getItemCount().intValue();
    }
}
