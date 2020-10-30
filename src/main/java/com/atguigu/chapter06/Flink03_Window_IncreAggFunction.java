package com.atguigu.chapter06;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

/**
 * TODO
 *
 * @author cjp
 * @version 1.0
 * @date 2020/10/30 11:27
 */
public class Flink03_Window_IncreAggFunction {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 1.读取数据
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordAndOneDS = env
                .socketTextStream("localhost", 9999)
                .map(new MapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> map(String value) throws Exception {
                        return Tuple2.of(value, 1);
                    }
                });

        //
//        socketDS.timeWindowAll()
        //
        KeyedStream<Tuple2<String, Integer>, String> wordAndOneKS = wordAndOneDS.keyBy(data -> data.f0);

        // TODO 窗口的增量聚合函数
        WindowedStream<Tuple2<String, Integer>, String, TimeWindow> wordAndOneWS = wordAndOneKS
                .timeWindow(Time.seconds(5)); // 滚动窗口

        wordAndOneWS
//                .reduce(
//                        new ReduceFunction<Tuple2<String, Integer>>() {
//                            @Override
//                            public Tuple2<String, Integer> reduce(Tuple2<String, Integer> value1, Tuple2<String, Integer> value2) throws Exception {
//                                System.out.println(value1 + "<---->" + value2);
//                                return Tuple2.of(value1.f0, value1.f1 + value2.f1);
//                            }
//                        }
//                )
                .aggregate(
                        new AggregateFunction<Tuple2<String,Integer>, Long, Long>() {
                            /**
                             * 初始化累加器，根据指定的累加器类型进行初始化
                             * @return
                             */
                            @Override
                            public Long createAccumulator() {
                                System.out.println("create....");
                                return 0L;
                            }

                            /**
                             * 定义 聚合操作：每条数据 跟 累加器 的聚合行为
                             * @param value
                             * @param accumulator
                             * @return
                             */
                            @Override
                            public Long add(Tuple2<String, Integer> value, Long accumulator) {
                                System.out.println("add...");
                                return value.f1 + accumulator;
                            }

                            /**
                             * 获取结果
                             * @param accumulator
                             * @return
                             */
                            @Override
                            public Long getResult(Long accumulator) {
                                System.out.println("get result...");
                                return accumulator;
                            }

                            /**
                             * 合并不同窗口的累加器 => 会话窗口才会调用
                             * @param a
                             * @param b
                             * @return
                             */
                            @Override
                            public Long merge(Long a, Long b) {
                                System.out.println("merge...");
                                return a+b;
                            }
                        }
                )
                .print();

        env.execute();
    }
}
