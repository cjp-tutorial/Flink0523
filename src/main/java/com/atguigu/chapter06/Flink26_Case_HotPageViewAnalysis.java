package com.atguigu.chapter06;

import com.atguigu.bean.*;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;

/**
 * 实时热门页面浏览排名 -- 每隔5s 输出 最近 10 minutes
 *
 * @author cjp
 * @version 1.0
 * @date 2020/10/28 15:26
 */
public class Flink26_Case_HotPageViewAnalysis {
    public static void main(String[] args) throws Exception {
        // 0. 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // 1.准备数据：读取、转换、抽取事件时间、生成watermark
        SingleOutputStreamOperator<ApacheLog> logDS = env
                .readTextFile("input/apache.log")
                .map(
                        new MapFunction<String, ApacheLog>() {
                            @Override
                            public ApacheLog map(String value) throws Exception {
                                String[] datas = value.split(" ");
                                // 对 日期 进行转换
                                SimpleDateFormat sdf = new SimpleDateFormat("dd/MM/yyyy:HH:mm:SS");
                                Date date = sdf.parse(datas[3]);
                                long time = date.getTime();
                                return new ApacheLog(
                                        datas[0],
                                        datas[1],
                                        time,
                                        datas[5],
                                        datas[6]
                                );
                            }
                        }
                )
                .assignTimestampsAndWatermarks(
                        new BoundedOutOfOrdernessTimestampExtractor<ApacheLog>(Time.minutes(1)) {
                            @Override
                            public long extractTimestamp(ApacheLog element) {
                                return element.getEventTime();
                            }
                        }
                );

        // 2.处理数据
        // 2.1 按照 统计维度 分组（页面 url）
        KeyedStream<ApacheLog, String> logKS = logDS.keyBy(data -> data.getUrl());
        // 2.2 开窗
        WindowedStream<ApacheLog, String, TimeWindow> logWS = logKS.timeWindow(Time.minutes(10), Time.seconds(5));
        // 2.3 求和
        SingleOutputStreamOperator<PageCountWithWindow> aggDS = logWS.aggregate(new SimpleAggFunction<ApacheLog>(),
                new ProcessWindowFunction<Long, PageCountWithWindow, String, TimeWindow>() {
                    @Override
                    public void process(String s, Context context, Iterable<Long> elements, Collector<PageCountWithWindow> out) throws Exception {
                        out.collect(new PageCountWithWindow(s, elements.iterator().next(), context.window().getEnd()));
                    }
                }
        );

        // 2.4 按照 窗口结束时间 分组


        // 2.5 用 process 排序


        // 4.执行
        env.execute();
    }
}
