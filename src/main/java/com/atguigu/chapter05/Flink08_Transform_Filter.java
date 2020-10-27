package com.atguigu.chapter05;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.Arrays;

/**
 * TODO
 *
 * @author cjp
 * @version 1.0
 * @date 2020/10/26 10:33
 */
public class Flink08_Transform_Filter {
    public static void main(String[] args) throws Exception {
        // 0.创建 流式的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env
                .fromCollection(Arrays.asList(1, 2, 3, 4))
//                .filter(new FilterFunction<Integer>() {
//                    @Override
//                    public boolean filter(Integer value) throws Exception {
//                        return value % 2 == 0;
//                    }
//                })
                .filter(data -> data % 2 == 0)
                .print();

        // 4.启动
        env.execute();
    }
}
