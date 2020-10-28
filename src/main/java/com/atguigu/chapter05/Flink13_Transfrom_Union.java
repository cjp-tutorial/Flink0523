package com.atguigu.chapter05;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

import java.util.Arrays;

/**
 * TODO
 *
 * @author cjp
 * @version 1.0
 * @date 2020/10/27 14:34
 */
public class Flink13_Transfrom_Union {
    public static void main(String[] args) throws Exception {
        // 0.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 1.读取数据
        DataStreamSource<Integer> numDS = env.fromCollection(Arrays.asList(1, 2, 3, 4));
        DataStreamSource<Integer> numDS2 = env.fromCollection(Arrays.asList(5, 6, 7, 8));
        DataStreamSource<Integer> numDS3 = env.fromCollection(Arrays.asList(15, 16, 17, 18));

        // TODO union 连接
        // 1.流的数据类型必须一致
        // 2.可以连接多条流， 得到的结果还是一个普通的 DataStream
        numDS
                .union(numDS2)
                .union(numDS3)
                .print();

        env.execute();
    }

}
