package com.atguigu.chapter06;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.*;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import javax.annotation.Nullable;

/**
 * 水位高于 50 的放入侧输出流，其他正常在 主流里
 *
 * @author cjp
 * @version 1.0
 * @date 2020/10/30 14:34
 */
public class Flink18_State_KeyedStateDemo {

    public static void main(String[] args) throws Exception {
        // 0.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // 1.读取数据
        SingleOutputStreamOperator<WaterSensor> sensorDS = env
//                .readTextFile("input/sensor-data.log")
                .socketTextStream("localhost", 9999)
                .map(new MapFunction<String, WaterSensor>() {
                    @Override
                    public WaterSensor map(String value) throws Exception {
                        String[] datas = value.split(",");
                        return new WaterSensor(datas[0], Long.valueOf(datas[1]), Integer.valueOf(datas[2]));
                    }
                })
                .assignTimestampsAndWatermarks(
                        new AssignerWithPunctuatedWatermarks<WaterSensor>() {

                            @Override
                            public long extractTimestamp(WaterSensor element, long previousElementTimestamp) {
                                return element.getTs() * 1000L;
                            }

                            @Nullable
                            @Override
                            public Watermark checkAndGetNextWatermark(WaterSensor lastElement, long extractedTimestamp) {
                                return new Watermark(extractedTimestamp);
                            }
                        }
                );

        // 2.处理数据
        OutputTag<String> highTag = new OutputTag<String>("high-level") {
        };
        SingleOutputStreamOperator<WaterSensor> resultDS = sensorDS
                .keyBy(sensor -> sensor.getId())
                .process(
                        new KeyedProcessFunction<String, WaterSensor, WaterSensor>() {
                            //     |                       ValueState<Integer> valueState=getRuntimeContext().getState();
                            // 1.定义状态；
                            ValueState<Integer> valueState;
                            ListState<Long> listState;
                            MapState<String, String> mapState;

                            @Override
                            public void open(Configuration parameters) throws Exception {
                                // 2. 在open中初始化状态
                                valueState = getRuntimeContext().getState(new ValueStateDescriptor<Integer>("value-state", Integer.class));
                                listState = getRuntimeContext().getListState(new ListStateDescriptor<Long>("list-state", Long.class));
                                mapState = getRuntimeContext().getMapState(new MapStateDescriptor<String, String>("map-state", String.class, String.class));
                            }

                            @Override
                            public void processElement(WaterSensor value, Context ctx, Collector<WaterSensor> out) throws Exception {
                                // 3.使用状态
//                                valueState.value(); // 取值
//                                valueState.update(); // 更新
//                                valueState.clear(); //清空
//
//                                listState.add(); // 添加单个值
//                                listState.addAll(); // 添加一个 list
//                                listState.update(); // 更新整个list
//                                listState.clear(); // 清空
//
//                                mapState.put(, ); // 添加一个 kv对
//                                mapState.putAll();      // 添加整个map
//                                mapState.get(); // 根据 某个 key  获取 value
//                                mapState.remove();      //删除指定 key 的数据
//                                mapState.clear();
                            }
                        }
                );

        resultDS.print("main-stream");

        DataStream<String> sideOutput = resultDS.getSideOutput(highTag);
        sideOutput.print("side-output");


        env.execute();
    }
}
