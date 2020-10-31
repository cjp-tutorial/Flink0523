package com.atguigu.chapter06;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * TODO
 *
 * @author cjp
 * @version 1.0
 * @date 2020/10/30 14:34
 */
public class Flink14_ProcessFunction_Keyed {
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
                        new BoundedOutOfOrdernessTimestampExtractor<WaterSensor>(Time.seconds(3)) {
                            @Override
                            public long extractTimestamp(WaterSensor element) {
                                return element.getTs() * 1000L;
                            }
                        }
                );

        // 2.处理数据
        sensorDS
                .keyBy(sensor -> sensor.getId())
                .process(
                        new KeyedProcessFunction<String, WaterSensor, String>() {
                            @Override
                            public void processElement(WaterSensor value, Context ctx, Collector<String> out) throws Exception {
                                // 获取时间戳：数据的事件时间
                                ctx.timestamp();
//                                System.out.println(value + ",ts=" + ctx.timestamp() + ",wm=" + ctx.timerService().currentWatermark());
//                                out.collect();

                                // 获取当前 key
                                ctx.getCurrentKey();
                                // 把数据放入侧输出流 => 第一个参数：标签对象； 第二个参数，要放入侧输出流的数据
                                ctx.output(, );

                                // 定时器
                                // 获取时间： 处理时间、 watermark
                                ctx.timerService().currentWatermark();
                                ctx.timerService().currentProcessingTime();

                                ctx.timerService().registerEventTimeTimer();
                                ctx.timerService().registerProcessingTimeTimer();

                                ctx.timerService().deleteEventTimeTimer();
                                ctx.timerService().deleteProcessingTimeTimer();


                            }
                        }
                )
                .print();


        env.execute();
    }
}
