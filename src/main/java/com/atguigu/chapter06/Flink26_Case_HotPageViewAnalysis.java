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

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.*;

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
        KeyedStream<PageCountWithWindow, Long> aggKS = aggDS.keyBy(data -> data.getWindowEnd());

        // 2.5 用 process 排序
        aggKS.process(new TopNPageView(5)).print();


        // 4.执行
        env.execute();
    }

    public static class TopNPageView extends KeyedProcessFunction<Long, PageCountWithWindow, String> {
        ListState<PageCountWithWindow> dataList;
        ValueState<Long> timerTs;

        private int threshold;

        public TopNPageView(int threshold) {
            this.threshold = threshold;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            dataList = getRuntimeContext().getListState(new ListStateDescriptor<PageCountWithWindow>("dataList", PageCountWithWindow.class));
            timerTs = getRuntimeContext().getState(new ValueStateDescriptor<Long>("timerTs", Long.class, 0L));
        }

        @Override
        public void processElement(PageCountWithWindow value, Context ctx, Collector<String> out) throws Exception {
            //先存起来
            dataList.add(value);
            // 数据到齐了，开始排序 => 模拟窗口的触发 => 定时器，给个延迟
            if (timerTs.value() == 0) {
                ctx.timerService().registerEventTimeTimer(value.getWindowEnd() + 1L);
                timerTs.update(value.getWindowEnd() + 1L);
            }
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            // 从 状态 取出 数据
            List<PageCountWithWindow> datas = new ArrayList<>();
            for (PageCountWithWindow pageCountWithWindow : dataList.get()) {
                datas.add(pageCountWithWindow);
            }
            // 清空状态
            dataList.clear();
            timerTs.clear();

            // 排序
            datas.sort(new Comparator<PageCountWithWindow>() {
                @Override
                public int compare(PageCountWithWindow o1, PageCountWithWindow o2) {
                    long result = o2.getPageCount() - o1.getPageCount();
                    if (result < 0) {
                        return -1;
                    } else if (result > 0) {
                        return 1;
                    } else {
                        return 0;
                    }
                }
            });

            // 取前 N
            StringBuffer resultStr = new StringBuffer();
            Timestamp windowEndDate = new Timestamp(timestamp - 1);
            resultStr.append("窗口结束时间:" + windowEndDate + "\n");

            for (int i = 0; i < (threshold > datas.size() ? datas.size() : threshold); i++) {
                resultStr.append("Top" + (i + 1) + ":" + datas.get(i) + "\n");
            }
            resultStr.append("=================================================================\n\n\n");
            out.collect(resultStr.toString());

        }
    }
}
