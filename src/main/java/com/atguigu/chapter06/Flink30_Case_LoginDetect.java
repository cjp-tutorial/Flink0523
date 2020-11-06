package com.atguigu.chapter06;

import com.atguigu.bean.LoginEvent;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * 恶意登陆检测： 2s内连续 2次登陆失败
 *
 * @author cjp
 * @version 1.0
 * @date 2020/10/28 16:39
 */
public class Flink30_Case_LoginDetect {
    public static void main(String[] args) throws Exception {
        // 0.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // 1. 读取数据
        SingleOutputStreamOperator<LoginEvent> loginDS = env
                .readTextFile("input/LoginLog.csv")
                .map(new MapFunction<String, LoginEvent>() {
                    @Override
                    public LoginEvent map(String value) throws Exception {
                        String[] datas = value.split(",");
                        return new LoginEvent(
                                Long.valueOf(datas[0]),
                                datas[1],
                                datas[2],
                                Long.valueOf(datas[3])
                        );
                    }
                })
                .assignTimestampsAndWatermarks(
                        new BoundedOutOfOrdernessTimestampExtractor<LoginEvent>(Time.seconds(10)) {
                            @Override
                            public long extractTimestamp(LoginEvent element) {
                                return element.getEventTime() * 1000L;
                            }
                        }
                );

        // 2.处理数据：2s内连续 2次登陆失败
        // 2.1 按照 统计维度 分组： 用户
        KeyedStream<LoginEvent, Long> loginKS = loginDS.keyBy(data -> data.getUserId());
        // 2.2 使用 process 处理
        loginKS.process(
                new KeyedProcessFunction<Long, LoginEvent, String>() {
                    ListState<LoginEvent> failLogins;
                    ValueState<Long> timerTs;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        failLogins = getRuntimeContext().getListState(new ListStateDescriptor<LoginEvent>("failLogins", LoginEvent.class));
                        timerTs = getRuntimeContext().getState(new ValueStateDescriptor<Long>("timerTs", Long.class));
                    }

                    @Override
                    public void processElement(LoginEvent value, Context ctx, Collector<String> out) throws Exception {
                        // 判断来的数据是成功还是失败
                        if ("success".equals(value.getEventType())) {
                            // 1.来的是 成功 的数据
                            // 1.1 不管满不满足条件，都要删除定时器
                            if (timerTs.value() != null) {
                                ctx.timerService().deleteEventTimeTimer(timerTs.value());
                            }
                            timerTs.clear();
                            // 1.2 判断一下 listState里面存了几个失败数据
                            if (failLogins.get().spliterator().estimateSize() >= 2) {
                                out.collect("用户" + value.getUserId() + "在2s内，连续登陆失败次数超过阈值2，可能存在风险！！！");
                            }
                            failLogins.clear();

                        } else {
                            // 2.来的是 失败 的数据 => 添加到 ListState，并且注册一个 2s的定时器
                            failLogins.add(value);
                            if (timerTs.value() == null) {
                                ctx.timerService().registerEventTimeTimer(ctx.timestamp() + 2000L);
                                timerTs.update(ctx.timestamp() + 2000L);
                            }
                        }
                    }


                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
                        // 判断一下 失败 次数
                        Iterable<LoginEvent> loginEvents = failLogins.get();
                        int failCount = 0;
                        for (LoginEvent loginEvent : loginEvents) {
                            failCount++;
                        }

                        if (failCount >= 2) {
                            out.collect("用户" + ctx.getCurrentKey() + "在2s内，连续登陆失败" + failCount + "次,超过阈值2，可能存在风险！！！");
                        }

                        failLogins.clear();
                        timerTs.clear();

                    }
                }
        )
                .print();


        env.execute();
    }


}
