package com.atguigu.bean;

import lombok.Data;

/**
 * TODO
 *
 * @author cjp
 * @version 1.0
 * @date 2020/11/3 11:48
 */
@Data
public class PageCountWithWindow {
    private String url;
    private Long pageCount;
    private Long windowEnd;

    public PageCountWithWindow(String url, Long pageCount, Long windowEnd) {
        this.url = url;
        this.pageCount = pageCount;
        this.windowEnd = windowEnd;
    }
}
