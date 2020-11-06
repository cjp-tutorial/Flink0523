package com.atguigu.chapter06;

import com.atguigu.bean.OrderEvent;
import com.atguigu.bean.TxEvent;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.Map;

/**
 * 订单支付超时
 *
 * @author cjp
 * @version 1.0
 * @date 2020/10/28 16:39
 */
public class Flink31_Case_OrderTimeoutDetect {
    public static void main(String[] args) throws Exception {
        // 0.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // 1. 读取数据
        // 1.1 读取 业务系统的 数据
        SingleOutputStreamOperator<OrderEvent> orderDS = env
                .readTextFile("input/OrderLog.csv")
                .map(
                        new MapFunction<String, OrderEvent>() {
                            @Override
                            public OrderEvent map(String value) throws Exception {
                                String[] datas = value.split(",");
                                return new OrderEvent(
                                        Long.valueOf(datas[0]),
                                        datas[1],
                                        datas[2],
                                        Long.valueOf(datas[3])
                                );
                            }
                        }
                )
                .assignTimestampsAndWatermarks(
                        new AscendingTimestampExtractor<OrderEvent>() {
                            @Override
                            public long extractAscendingTimestamp(OrderEvent element) {
                                return element.getEventTime() * 1000L;
                            }
                        }
                );


        // 2. 处理数据
        // 2.1 按照 统计维度 分组：订单
        KeyedStream<OrderEvent, Long> orderKS = orderDS.keyBy(data -> data.getOrderId());
        // 2.2 使用 process
        orderKS.process(
                new KeyedProcessFunction<Long, OrderEvent, String>() {

                    ValueState<OrderEvent> payState;
                    ValueState<OrderEvent> createState;
                    ValueState<Long> timerTs;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        payState = getRuntimeContext().getState(new ValueStateDescriptor<OrderEvent>("payState", OrderEvent.class));
                        createState = getRuntimeContext().getState(new ValueStateDescriptor<OrderEvent>("createState", OrderEvent.class));
                        timerTs = getRuntimeContext().getState(new ValueStateDescriptor<Long>("timerTs", Long.class));
                    }

                    @Override
                    public void processElement(OrderEvent value, Context ctx, Collector<String> out) throws Exception {
                        // TODO 1.考虑 只有一个数据来 的情况（只有create来，或只有pay来）
                        if (timerTs.value() == null) {
                            // 该订单的 第一条数据来的时候，注册一个定时器 => 等另一半
                            ctx.timerService().registerEventTimeTimer(ctx.timestamp() + 15 * 60 * 1000L);
                            timerTs.update(ctx.timestamp() + 15 * 60 * 1000L);
                        } else {
                            // 说明 该订单 另一条数据来了，就是正常的下单和支付，可以删掉定时器
                            ctx.timerService().deleteEventTimeTimer(timerTs.value());
                            timerTs.clear();
                        }

                        // TODO 2. 考虑 两个数据 都会来
                        // 因为数据是乱序的，不一定谁先来，判断来的是 create 还是 pay？
                        if ("create".equals(value.getEventType())) {
                            // 1. 来的是 create => 判断 pay 是否来过
                            if (payState.value() == null) {
                                // 1.1 说明 pay 没来过 => 把自己（create）存起来
                                createState.update(value);
                            } else {
                                // 1.2 说明 pay 来过 => 判断一下是否超时
                                if (payState.value().getEventTime() - value.getEventTime() > 15 * 60) {
                                    // 1.2.1 支付超时
                                    out.collect("订单" + value.getOrderId() + "支付成功，但是超时，业务系统可能存在漏洞！！！");
                                } else {
                                    // 1.2.2 支付没超时
                                    out.collect("订单" + value.getOrderId() + "支付成功！！！");
                                }
                                // 清空保存的 pay，已经用完了
                                payState.clear();
                            }
                        } else {
                            // 2. 来的是 pay => 判断 create 是否来过
                            if (createState.value() == null) {
                                // 2.1 说明 create 没来过 => 把自己（pay）保存起来
                                payState.update(value);
                            } else {
                                // 2.2 说明 create 来过 => 判断一下是否超时
                                if (value.getEventTime() - createState.value().getEventTime() > 15 * 60) {
                                    // 2.2.1 支付超时
                                    out.collect("订单" + value.getOrderId() + "支付成功，但是超时，业务系统可能存在漏洞！！！");
                                } else {
                                    // 2.2.2 支付没超时
                                    out.collect("订单" + value.getOrderId() + "支付成功！！！");
                                }
                                // 清空保存的 create，已经用完了
                                createState.clear();
                            }
                        }
                    }

                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
                        // 定时器触发，说明 另一半 没来
                        if (payState.value() != null) {
                            // 1.说明 pay来过，那就是 create没来
                            out.collect("订单" + ctx.getCurrentKey() + "已支付，但是下单数据丢失，采集系统或业务系统可能存在问题！！！");
                            payState.clear();
                        }

                        if (createState.value() != null) {
                            // 1.说明 create来过，那就是 pay没来
                            out.collect("订单" + ctx.getCurrentKey() + "支付超时！！！");
                            createState.clear();
                        }

                        timerTs.clear();
                    }
                }
        )
                .print();


        env.execute();
    }

}
