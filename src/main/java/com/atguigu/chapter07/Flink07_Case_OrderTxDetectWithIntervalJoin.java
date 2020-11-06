package com.atguigu.chapter07;

import com.atguigu.bean.OrderEvent;
import com.atguigu.bean.TxEvent;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.Map;

/**
 * 实时对账
 *
 * @author cjp
 * @version 1.0
 * @date 2020/10/28 16:39
 */
public class Flink07_Case_OrderTxDetectWithIntervalJoin {
    public static void main(String[] args) throws Exception {
        // 0.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
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
        // 1.2 读取 交易系统的 数据
        SingleOutputStreamOperator<TxEvent> txDS = env
                .readTextFile("input/ReceiptLog.csv")
                .map(
                        new MapFunction<String, TxEvent>() {
                            @Override
                            public TxEvent map(String value) throws Exception {
                                String[] datas = value.split(",");
                                return new TxEvent(
                                        datas[0],
                                        datas[1],
                                        Long.valueOf(datas[2])
                                );
                            }
                        }
                )
                .assignTimestampsAndWatermarks(
                        new AscendingTimestampExtractor<TxEvent>() {
                            @Override
                            public long extractAscendingTimestamp(TxEvent element) {
                                return element.getEventTime() * 1000L;
                            }
                        }
                );

        // 2. 处理数据
        // TODO Interval Join实现 实时对账
        // 底层用 connect 实现 => keyby
        // 两条流的数据，会分别使用各自的  MapState进行存储， key是 事件事件，value是数据值
        orderDS.keyBy(order -> order.getTxId())
                .intervalJoin(txDS.keyBy(tx -> tx.getTxId()))
                .between(Time.hours(-5), Time.hours(5))
                .process(
                        new ProcessJoinFunction<OrderEvent, TxEvent, String>() {
                            @Override
                            public void processElement(OrderEvent left, TxEvent right, Context ctx, Collector<String> out) throws Exception {
                                // 每次进来一组， （基准数据 跟 另一条流里 界限范围内的数据，相同 key 的数据 轮流发生关系）
                                out.collect(left + "->" + right);
                            }
                        }
                )
                .print();


        env.execute();
    }


}
