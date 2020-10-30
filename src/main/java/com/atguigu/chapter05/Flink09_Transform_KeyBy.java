package com.atguigu.chapter05;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;

/**
 * TODO
 *
 * @author cjp
 * @version 1.0
 * @date 2020/10/26 10:33
 */
public class Flink09_Transform_KeyBy {
    public static void main(String[] args) throws Exception {
        // 0.创建 流式的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 1.读取数据，转成 WaterSensor类型
        DataStreamSource<String> fileDS = env.readTextFile("input/sensor-data.log");

        SingleOutputStreamOperator<WaterSensor> sensorDS = fileDS.map(new MapFunction<String, WaterSensor>() {
            @Override
            public WaterSensor map(String value) throws Exception {
                String[] datas = value.split(",");
                return new WaterSensor(datas[0], Long.valueOf(datas[1]), Integer.valueOf(datas[2]));
            }
        });

        // 2. TODO KeyBy分组
        // 分组，是给 数据打上 分组的标签
        // 分组=》 同一分组 的数据 在一起 => 同一个 slot、subtask 可以有多个分组

        // 根据 位置索引 分组 => 无法推断类型，所以key返回的是 tuple
//        KeyedStream<WaterSensor, Tuple> resultKS = sensorDS.keyBy(0);
        // 根据 属性名称 分组 => 无法推断类型，所以key返回的是 tuple
        // 使用要求：提供空参构造器
        KeyedStream<WaterSensor, Tuple> resultKS = sensorDS.keyBy("id");
//        KeyedStream<WaterSensor, String> resultKS = sensorDS.keyBy(new MyKeySelecor());
//        KeyedStream<WaterSensor, String> resultKS = sensorDS
//                .keyBy(data -> data.getId());

        // TODO 源码分析
//        默认的 MAX_PARALLELISM = 128
//        key会经过两次hash（hashcode， murmurhash），并与 MAX_PARALLELISM 取模 得到一个 ID
        // 发往下游哪个并行子任务，由selectChannel()决定 = id * 并行度 / MAX_PARALLELISM

        resultKS.print();


        // 4.启动
        env.execute();
    }

    public static class MyKeySelecor implements KeySelector<WaterSensor, String> {

        @Override
        public String getKey(WaterSensor value) throws Exception {
            return value.getId();
        }
    }
}
