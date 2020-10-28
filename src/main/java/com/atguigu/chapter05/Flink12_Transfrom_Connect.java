package com.atguigu.chapter05;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.SplitStream;
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
public class Flink12_Transfrom_Connect {
    public static void main(String[] args) throws Exception {
        // 0.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 1.读取数据
        SingleOutputStreamOperator<WaterSensor> sensorDS = env
                .readTextFile("input/sensor-data.log")
                .map(new MapFunction<String, WaterSensor>() {
                    @Override
                    public WaterSensor map(String value) throws Exception {
                        String[] datas = value.split(",");
                        return new WaterSensor(datas[0], Long.valueOf(datas[1]), Integer.valueOf(datas[2]));
                    }
                });

        DataStreamSource<Integer> numDS = env.fromCollection(Arrays.asList(1, 2, 3, 4));
        DataStreamSource<Integer> numDS2 = env.fromCollection(Arrays.asList(5, 6, 7, 8));

        numDS
                .connect(numDS2)
                .map(new CoMapFunction<Integer, Integer, Object>() {
                    @Override
                    public Object map1(Integer value) throws Exception {
                        return "ds1:"+value;
                    }

                    @Override
                    public Object map2(Integer value) throws Exception {
                        return "ds2:"+value;
                    }
                })
                .print();



        //TODO Connect  流的连接操作，同床异梦
        // 1.只能对两条流进行连接
        // 2.数据类型可以不一样
        // 3.各自处理各自的数据
//        ConnectedStreams<WaterSensor, Integer> resultCS = sensorDS.connect(numDS);
//
//        resultCS
//                .map(
//                        new CoMapFunction<WaterSensor, Integer, Object>() {
//                            @Override
//                            public Object map1(WaterSensor value) throws Exception {
//                                return value.toString();
//                            }
//
//                            @Override
//                            public Object map2(Integer value) throws Exception {
//                                return value * 2;
//                            }
//                        }
//                )
//                .print();


        env.execute();
    }

}
