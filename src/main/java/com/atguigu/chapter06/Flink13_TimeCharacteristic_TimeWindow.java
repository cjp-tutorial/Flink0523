package com.atguigu.chapter06;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

/**
 * TODO
 *
 * @author cjp
 * @version 1.0
 * @date 2020/10/30 11:27
 */
public class Flink13_TimeCharacteristic_TimeWindow {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

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
//        wordAndOneDS.timeWindowAll()
        //
        KeyedStream<Tuple2<String, Integer>, String> wordAndOneKS = wordAndOneDS.keyBy(data -> data.f0);

        // TODO TimeWindow API
        WindowedStream<Tuple2<String, Integer>, String, TimeWindow> wordAndOneWS = wordAndOneKS
//                .timeWindow(Time.seconds(5)); // 滚动窗口
//                    .window(TumblingProcessingTimeWindows.of(Time.seconds(5)));
//                .timeWindow(Time.seconds(5),Time.seconds(2)); // 滑动窗口
//                .window(SlidingProcessingTimeWindows.of(Time.seconds(5), Time.seconds(2)));
//        .window(ProcessingTimeSessionWindows.withGap(Time.seconds(3))); // 会话窗口
        .window(EventTimeSessionWindows.withGap(Time.seconds(3)));

        SingleOutputStreamOperator<Tuple2<String, Integer>> resultDS = wordAndOneWS.sum(1);

        resultDS.print();


        env.execute();
    }
}
