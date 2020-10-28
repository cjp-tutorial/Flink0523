package com.atguigu.chapter05;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;

/**
 * TODO
 *
 * @author cjp
 * @version 1.0
 * @date 2020/10/27 14:34
 */
public class Flink11_Transfrom_Split {
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

        // TODO split  vc < 50 正常， 50 < vc < 80 警告， vc > 80 告警
        SplitStream<WaterSensor> splitSS = sensorDS.split(
                new OutputSelector<WaterSensor>() {
                    @Override
                    public Iterable<String> select(WaterSensor value) {
                        if (value.getVc() < 50) {
                            return Arrays.asList("normal","gousheng");
                        } else if (value.getVc() < 80) {
                            return Arrays.asList("warn","gousheng");
                        } else {
                            return Arrays.asList("alarm");
                        }
                    }
                }
        );

        // TODO select
        splitSS.select("gousheng").print("normal");
        splitSS.select("warn").print("warn");
        splitSS.select("alarm").print("alarm");


        env.execute();
    }

}
