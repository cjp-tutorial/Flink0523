package com.atguigu.chapter02;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * TODO
 *
 * @author cjp
 * @version 1.0
 * @date 2020/10/26 10:42
 */
public class Flink03_WC_UnBoundedStream {
    public static void main(String[] args) throws Exception {
        // 0.创建 流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 1.读取数据 => 无界流：kafka、socket
//        DataStreamSource<String> socketDS = env.socketTextStream("localhost", 9999);
        DataStreamSource<String> socketDS = env.socketTextStream("hadoop102", 9999);

        // 2.处理数据
        // 2.1 扁平化操作
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordAndOneDS = socketDS.flatMap(
                new FlatMapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                        String[] words = value.split(" ");
                        for (String word : words) {
                            Tuple2<String, Integer> tuple2 = Tuple2.of(word, 1);
                            // 采集器
                            out.collect(tuple2);
                        }
                    }
                }
        );
        // 2.2 分组
        KeyedStream<Tuple2<String, Integer>, Tuple> wordAndOneKS = wordAndOneDS.keyBy(0);
        // 2.3 聚合
        SingleOutputStreamOperator<Tuple2<String, Integer>> resultDS = wordAndOneKS.sum(1);

        // 3.输出
        resultDS.print();

        // 4.执行
        env.execute();
    }
}
