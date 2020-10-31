package com.atguigu.chapter06;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * TODO
 *
 * @author cjp
 * @version 1.0
 * @date 2020/10/30 14:34
 */
public class Flink12_Watermark_Sumary {
    public static void main(String[] args) throws Exception {
        //TODO Watermark总结
        // 1.概念和理解
        //  => 解决乱序的问题
        //  => 表示 事件时间 的进展
        //  => 是一个特殊的时间戳 (插入到数据流里，随着流而往下游传递)
        //  => 单调递增的
        //  => 认为 在它 之前的数据已经处理过了

        // 2.官方提供的实现
        //  => 升序 watermark = EventTime - 1ms
        //  => 乱序 watermark = EventTime - 最大乱序程度（自己设的）

        // 3.自定义生成 watermark
        //  => 周期性，默认周期 200ms，可以修改
        //  => 间歇性，来一条，生成一次 watermark

        // 4.watermark的传递、多并行度的影响
        //  以 多个 并行子任务 中，watermark最小的为准 => 参考 木桶原理，因为最小的watermark之前肯定处理了

        // 5.有界流的问题
        //  有界流在关闭之前，会把 watermark设为 Long的最大值 => 目的是为了保证所有的窗口都被触发、所有的数据都被计算


        // TODO Flink对乱序和迟到数据的处理、保证
        // 1.watermark设置 乱序程度（等待时间） => 解决乱序
        // 2.窗口允许迟到 => 窗口再等待一会，处理关窗前迟到数据
        // 3.侧输出流 => 关窗后的迟到数据
    }
}
