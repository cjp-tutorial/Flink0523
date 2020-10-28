package com.atguigu.chapter05;

import com.atguigu.bean.UserBehavior;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * TODO
 *
 * @author cjp
 * @version 1.0
 * @date 2020/10/28 15:26
 */
public class Flink23_Case_PVByFlatMap {
    public static void main(String[] args) throws Exception {
        // 0. 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 1.读取数据
        env
                .readTextFile("input/UserBehavior.csv")
                .flatMap(
                        new FlatMapFunction<String, Tuple2<String, Integer>>() {
                            @Override
                            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                                String datas = value.split(",")[3];
                                if ("pv".equals(datas)) {
                                    out.collect(Tuple2.of("pv", 1));
                                }
                            }
                        }
                )
                .keyBy(data -> data.f0)
                .sum(1)
                .print();

        // 4.执行
        env.execute();
    }
}
