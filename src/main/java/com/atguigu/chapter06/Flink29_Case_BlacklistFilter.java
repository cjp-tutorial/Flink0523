package com.atguigu.chapter06;

import com.atguigu.bean.AdClickCountByUserWithWindowEnd;
import com.atguigu.bean.AdClickLog;
import com.atguigu.bean.SimpleAggFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * 黑名单过滤：一天内 对 同一个广告 点击 超过 100次 加入黑名单
 *
 * @author cjp
 * @version 1.0
 * @date 2020/10/28 16:39
 */
public class Flink29_Case_BlacklistFilter {
    public static void main(String[] args) throws Exception {
        // 0.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // 1. 读取数据
        SingleOutputStreamOperator<AdClickLog> adClickDS = env
                .readTextFile("input/AdClickLog.csv")
                .map(
                        new MapFunction<String, AdClickLog>() {
                            @Override
                            public AdClickLog map(String value) throws Exception {
                                String[] datas = value.split(",");
                                return new AdClickLog(
                                        Long.valueOf(datas[0]),
                                        Long.valueOf(datas[1]),
                                        datas[2],
                                        datas[3],
                                        Long.valueOf(datas[4])
                                );
                            }
                        }
                )
                .assignTimestampsAndWatermarks(
                        new AscendingTimestampExtractor<AdClickLog>() {
                            @Override
                            public long extractAscendingTimestamp(AdClickLog element) {
                                return element.getTimestamp() * 1000L;
                            }
                        }
                );

        // 2.处理数据
        // 2.1 按照 统计维度 分组 （用户、广告）
        KeyedStream<AdClickLog, Tuple2<Long, Long>> adClickLogTuple2KeyedStream = adClickDS.keyBy(new KeySelector<AdClickLog, Tuple2<Long, Long>>() {
            @Override
            public Tuple2<Long, Long> getKey(AdClickLog value) throws Exception {
                return Tuple2.of(value.getUserId(), value.getAdId());
            }
        });
        // 2.2
        OutputTag<String> blackTag = new OutputTag<String>("blacklist-alarm") {
        };
        SingleOutputStreamOperator<AdClickLog> blackFilterDS = adClickLogTuple2KeyedStream.process(new BlacklistFilter());
        blackFilterDS.getSideOutput(blackTag).print("black-alarm");

        //
        blackFilterDS
                .keyBy(new KeySelector<AdClickLog, Tuple2<Long, Long>>() {
                    @Override
                    public Tuple2<Long, Long> getKey(AdClickLog value) throws Exception {
                        return Tuple2.of(value.getUserId(), value.getAdId());
                    }
                })
                .timeWindow(Time.hours(1), Time.minutes(5))
                .aggregate(new SimpleAggFunction<AdClickLog>(),
                        new ProcessWindowFunction<Long, AdClickCountByUserWithWindowEnd, Tuple2<Long, Long>, TimeWindow>() {
                            @Override
                            public void process(Tuple2<Long, Long> key, Context context, Iterable<Long> elements, Collector<AdClickCountByUserWithWindowEnd> out) throws Exception {
                                out.collect(new AdClickCountByUserWithWindowEnd(key.f1, key.f0, elements.iterator().next(), context.window().getEnd()));
                            }
                        })
                .keyBy(data -> data.getWindowEnd())
                .process(new Flink28_Case_AdClickByUserAnalysis.TopNAdClick(3))
                .print("topN");

        env.execute();
    }


    public static class BlacklistFilter extends KeyedProcessFunction<Tuple2<Long, Long>, AdClickLog, AdClickLog> {

        ValueState<Long> clickCount;
        ValueState<Boolean> alarmFlag;
        // 还是要用 状态保存时间 => 为了让每个分组都去注册定时器，这样，ontimer的时候才能每个分组去分别清空自己的保存值
        ValueState<Long> triggerTs;


        @Override
        public void open(Configuration parameters) throws Exception {
            clickCount = getRuntimeContext().getState(new ValueStateDescriptor<Long>("clickCount", Long.class, 0L));
            alarmFlag = getRuntimeContext().getState(new ValueStateDescriptor<Boolean>("alarmFlag", Boolean.class, false));
            triggerTs = getRuntimeContext().getState(new ValueStateDescriptor<Long>("triggerTs", Long.class));
        }

        @Override
        public void processElement(AdClickLog value, Context ctx, Collector<AdClickLog> out) throws Exception {
            Long currentClickCount = clickCount.value();

            // 隔天0点，应该对 count值清零 => 怎么获取 隔天 0点
            // 当天 第一条数据来的时候，获取隔天 0点的时间，进行 注册
            if (triggerTs.value() == null) {
                // 获取隔天 0点的时间
                // 1.获取 直到今天， 距离 1970年经过了多少天
                long untilNowDays = ctx.timestamp() / (24 * 60 * 60 * 1000L);
                // 2.获取 直到明天， 距离 1970年经过了多少天
                long untilTomorrowDays = untilNowDays + 1;
                // 3.获取 明天 0点， 距离 1970年经过了多少 毫秒
                triggerTs.update(untilTomorrowDays * (24 * 60 * 60 * 1000L));

                // 注册
                ctx.timerService().registerEventTimeTimer(triggerTs.value());
            }

            if (currentClickCount >= 100) {
                // 超过阈值，告警,只告警一次，用标志位判断
                if (!alarmFlag.value()) {
                    // 匿名内部类 => new 实体类的构造器（构造器的参数列表）{代码实现}
                    OutputTag<String> blackTag = new OutputTag<String>("blacklist-alarm") {
                    };
                    ctx.output(blackTag, "用户" + value.getUserId() + "对广告" + value.getAdId() + "今日点击超过阈值100次！！！");
                    alarmFlag.update(true);
                }
            } else {
                // 没超过 阈值，才统计
                clickCount.update(currentClickCount + 1);
                // 没超过阈值，属于正常的数据，往下游传递，进行业务分析
                out.collect(value);
            }

        }


        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<AdClickLog> out) throws Exception {
            // 说明，已经到了 隔天0点
            // 清空 count值，清空注册的时间,告警标志位清空
            clickCount.clear();
            alarmFlag.clear();
            triggerTs.clear();

            //TODO clear方法也是分组隔离的，只清空当前分组的值
            // 在定时器中，触发操作调用clear，那么就看这个定时器是谁注册的，那么key就对应是谁的
        }
    }

}
