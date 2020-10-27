package com.atguigu.chapter05;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
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
 * @date 2020/10/26 10:33
 */
public class Flink07_Transform_FlatMap {
    public static void main(String[] args) throws Exception {
        // 0.创建 流式的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 1.读取数据: 有界流 => 有头有尾 => 文件
        DataStreamSource<String> fileDS = env.readTextFile("input/sensor-data.log");

        // 2.处理数据
        fileDS
                .flatMap(
                        new FlatMapFunction<String, String>() {
                            @Override
                            public void flatMap(String value, Collector<String> out) throws Exception {
                                String[] datas = value.split(",");
                                for (String data : datas) {
                                    out.collect(data);
                                }
                            }
                        }
                )
                .print();


        // 4.启动
        env.execute();
    }
}
