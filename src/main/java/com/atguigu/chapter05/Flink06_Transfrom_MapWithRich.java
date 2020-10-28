package com.atguigu.chapter05;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * TODO
 *
 * @author cjp
 * @version 1.0
 * @date 2020/10/27 14:34
 */
public class Flink06_Transfrom_MapWithRich {
    public static void main(String[] args) throws Exception {
        // 0.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        // 1.读取数据
        DataStreamSource<String> fileDS = env.readTextFile("input/sensor-data.log");
//        DataStreamSource<String> fileDS = env.socketTextStream("localhost", 9999);

        // 2.map
        SingleOutputStreamOperator<WaterSensor> resultDS = fileDS.map(new MyRichMapFunction());

        resultDS.print();

        env.execute();
    }

    /**
     * 富函数
     * 富有体现在： 1. 有 open和close 生命周期方法 ； 2. 可以获取 运行时上下文 => 获取到一些环境信息、状态....
     *
     * 1. 继承 RichMapFunction，定义两个泛型
     * 2. 实现 map方法 ；  可以定义 open 和close， 可以获取 运行时上下文
     */
    public static class MyRichMapFunction extends RichMapFunction<String, WaterSensor> {

        @Override
        public WaterSensor map(String value) throws Exception {
            String[] datas = value.split(",");
            return new WaterSensor(getRuntimeContext().getTaskName(), Long.valueOf(datas[1]), Integer.valueOf(datas[2]));
        }


        @Override
        public void open(Configuration parameters) throws Exception {
            System.out.println("open......");
            ;
        }

        @Override
        public void close() throws Exception {
            System.out.println("close.....");
            ;
        }
    }

}
