package com.atguigu.chapter06;

import com.atguigu.bean.AdClickCountByUserWithWindowEnd;
import com.atguigu.bean.AdClickCountWithWindowEnd;
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
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * 实时统计 各省份的广告 点击情况
 *
 * @author cjp
 * @version 1.0
 * @date 2020/10/28 16:39
 */
public class Flink28_Case_AdClickByUserAnalysis {
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
        // 2.1 按照 统计维度 （用户、广告） 分组
        adClickDS
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
                .process(new TopNAdClick(3))
                .print();

        //
        env.execute();
    }

    public static class TopNAdClick extends KeyedProcessFunction<Long, AdClickCountByUserWithWindowEnd, String> {

        ListState<AdClickCountByUserWithWindowEnd> adClicks;
        ValueState<Long> triggerTs;

        private int threshold;

        public TopNAdClick(int threshold) {
            this.threshold = threshold;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            adClicks = getRuntimeContext().getListState(new ListStateDescriptor<AdClickCountByUserWithWindowEnd>("adClicks", AdClickCountByUserWithWindowEnd.class));
            triggerTs = getRuntimeContext().getState(new ValueStateDescriptor<Long>("triggerTs", Long.class));
        }

        @Override
        public void processElement(AdClickCountByUserWithWindowEnd value, Context ctx, Collector<String> out) throws Exception {
            // 存
            adClicks.add(value);

            // 注册
            if (triggerTs.value() == null) {
                ctx.timerService().registerEventTimeTimer(value.getWindowEnd() + 1L);
                triggerTs.update(value.getWindowEnd() + 1L);
            }
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            List<AdClickCountByUserWithWindowEnd> datas = new ArrayList<>();
            for (AdClickCountByUserWithWindowEnd adClickCountByUserWithWindowEnd : adClicks.get()) {
                datas.add(adClickCountByUserWithWindowEnd);
            }
            adClicks.clear();
            triggerTs.clear();

            datas.sort(new Comparator<AdClickCountByUserWithWindowEnd>() {
                @Override
                public int compare(AdClickCountByUserWithWindowEnd o1, AdClickCountByUserWithWindowEnd o2) {
                    return o2.getAdClickCount().intValue() - o1.getAdClickCount().intValue();
                }
            });

            StringBuffer resultStr = new StringBuffer();
            resultStr.append("窗口结束时间:" + new Timestamp(timestamp - 1) + "\n");

            for (int i = 0; i < (threshold > datas.size() ? datas.size() : threshold); i++) {
                resultStr.append("Top" + (i + 1) + ":" + datas.get(i) + "\n");
            }
            resultStr.append("=================================================================\n\n\n");
            out.collect(resultStr.toString());


        }
    }

}
