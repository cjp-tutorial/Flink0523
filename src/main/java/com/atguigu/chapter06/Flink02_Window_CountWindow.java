package com.atguigu.chapter06;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

/**
 * TODO
 *
 * @author cjp
 * @version 1.0
 * @date 2020/10/30 11:27
 */
public class Flink02_Window_CountWindow {
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
        KeyedStream<Tuple2<String, Integer>, String> wordAndOneKS = wordAndOneDS.keyBy(data -> data.f0);

        // TODO CountWindow API
        // 分组之后再开窗，那么窗口的关闭是看，相同分组的数据条数是否达到
        // 滑动窗口 => 每经过一个步长，都会有一个窗口关闭、输出
        // 在第一条数据之前，也是有窗口的，只不过是没有数据属于那个窗口
        WindowedStream<Tuple2<String, Integer>, String, GlobalWindow> wordAndOneWS = wordAndOneKS
//                .countWindow(5);
                .countWindow(5, 2);

        wordAndOneWS.sum(1).print();

        env.execute();
    }
}
