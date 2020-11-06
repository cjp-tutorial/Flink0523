package com.atguigu.chapter07;

import com.atguigu.bean.OrderEvent;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.PatternTimeoutFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.List;
import java.util.Map;

/**
 * 订单支付超时
 *
 * @author cjp
 * @version 1.0
 * @date 2020/10/28 16:39
 */
public class Flink05_Case_OrderTimeoutDetectWithCEP {
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

        // TODO 使用CEP实现
        // 1.定义规则
        Pattern<OrderEvent, OrderEvent> pattern = Pattern.<OrderEvent>begin("create")
                .where(new IterativeCondition<OrderEvent>() {
                    @Override
                    public boolean filter(OrderEvent value, Context<OrderEvent> ctx) throws Exception {
                        return "create".equals(value.getEventType());
                    }
                })
                .followedBy("pay")
                .where(new IterativeCondition<OrderEvent>() {
                    @Override
                    public boolean filter(OrderEvent value, Context<OrderEvent> ctx) throws Exception {
                        return "pay".equals(value.getEventType());
                    }
                })
                .within(Time.minutes(15));
//                .within(Time.milliseconds(15 * 60 * 1000 + 1));

        // 2.应用规则
        PatternStream<OrderEvent> orderPS = CEP.pattern(orderKS, pattern);

        // 3.获取结果:三个参数
        // 第一个参数：OutputTag， 超时的数据会放入侧输出流，所以需要一个 侧输出流标签
        // 第二个参数：PatternTimeoutFunction，超时数据的处理函数，会对超时的数据进行处理，之后 放入 侧输出流
        // 第三个参数：PatternSelectFunction，对匹配上的数据进行处理，输出到 主流
        OutputTag<String> timeoutTag = new OutputTag<String>("timeout") {
        };
        SingleOutputStreamOperator<String> resultDS = orderPS.select(
                timeoutTag,
                new PatternTimeoutFunction<OrderEvent, String>() {
                    @Override
                    public String timeout(Map<String, List<OrderEvent>> pattern, long timeoutTimestamp) throws Exception {
                        return pattern.toString() + "----->" + timeoutTimestamp;
                    }
                },
                new PatternSelectFunction<OrderEvent, String>() {
                    @Override
                    public String select(Map<String, List<OrderEvent>> pattern) throws Exception {
                        return pattern.toString();
                    }
                }
        );

//        resultDS.print("normal");
        resultDS.getSideOutput(timeoutTag).print("timeout");


        env.execute();
    }

}
