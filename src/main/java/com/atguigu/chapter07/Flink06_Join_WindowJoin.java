package com.atguigu.chapter07;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.List;
import java.util.Map;

/**
 * TODO
 *
 * @author cjp
 * @version 1.0
 * @date 2020/11/6 11:33
 */
public class Flink06_Join_WindowJoin {
    public static void main(String[] args) throws Exception {
        // 0.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);


        SingleOutputStreamOperator<Tuple2<Integer, Long>> numDS1 = env
                .fromElements(
                        Tuple2.of(1, 111000L),
                        Tuple2.of(2, 211000L),
                        Tuple2.of(3, 311000L),
                        Tuple2.of(4, 411000L)
                )
                .assignTimestampsAndWatermarks(
                        new AscendingTimestampExtractor<Tuple2<Integer, Long>>() {
                            @Override
                            public long extractAscendingTimestamp(Tuple2<Integer, Long> element) {
                                return element.f1;
                            }
                        }
                );

        SingleOutputStreamOperator<Tuple3<Integer, Long, String>> numDS2 = env
                .fromElements(
                        Tuple3.of(1, 211000L, "a"),
                        Tuple3.of(2, 211000L, "b"),
                        Tuple3.of(3, 311000L, "c"),
                        Tuple3.of(4, 411000L, "d")
                )
                .assignTimestampsAndWatermarks(
                        new AscendingTimestampExtractor<Tuple3<Integer, Long, String>>() {
                            @Override
                            public long extractAscendingTimestamp(Tuple3<Integer, Long, String> element) {
                                return element.f1;
                            }
                        }
                );

        //TODO Window Join
        // 同一个 时间窗口内，两条流中 key相同的 数据，放到一起，进行处理
        numDS1.join(numDS2)
                .where(data1 -> data1.f0)
                .equalTo(data2 -> data2.f0)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .apply(new JoinFunction<Tuple2<Integer, Long>, Tuple3<Integer, Long, String>, String>() {
                    @Override
                    public String join(Tuple2<Integer, Long> first, Tuple3<Integer, Long, String> second) throws Exception {
                        return first + "----->" + second;
                    }
                })
                .print();


        env.execute();
    }
}
