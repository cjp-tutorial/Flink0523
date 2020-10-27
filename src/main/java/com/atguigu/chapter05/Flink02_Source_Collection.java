package com.atguigu.chapter05;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;

/**
 * TODO
 *
 * @author cjp
 * @version 1.0
 * @date 2020/10/27 14:27
 */
public class Flink02_Source_Collection {
    public static void main(String[] args) throws Exception {
        // 0.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 1.从集合读取数据
        DataStreamSource<Integer> inputDS = env.fromCollection(Arrays.asList(1, 2, 3, 4));
//        env.fromElements()


        inputDS.print();

        //
        env.execute();
    }
}
