package com.atguigu.chapter06;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
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
public class Flink11_Watermark_SideOutput {
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
        // TODO 窗口关闭后 的 迟到数据 处理 --- 侧输出流
        // 1.在窗口真正关闭后，还有迟到数据，那么就放入到 侧输出流中


        // TODO 创建 OutputTag实例的时候，必须指定泛型，必须给花括号
        OutputTag<WaterSensor> late = new OutputTag<WaterSensor>("lateData"){};

        SingleOutputStreamOperator<String> resultDS = sensorDS
                .keyBy(sensor -> sensor.getId())
                .timeWindow(Time.seconds(5))
                .allowedLateness(Time.seconds(2))
                .sideOutputLateData(late)
                .process(
                        new ProcessWindowFunction<WaterSensor, String, String, TimeWindow>() {
                            /**
                             * 输入的数据是：整个窗口 同一分组 的数据 一起 处理
                             * @param s
                             * @param context
                             * @param elements
                             * @param out
                             * @throws Exception
                             */
                            @Override
                            public void process(String s, Context context, Iterable<WaterSensor> elements, Collector<String> out) throws Exception {
                                System.out.println("process...");
                                out.collect("当前key=" + s
                                        + "当前watermark=" + context.currentWatermark()
                                        + "窗口为[" + context.window().getStart() + "," + context.window().getEnd() + ")"
                                        + ",一共有" + elements.spliterator().estimateSize() + "条数据");
                            }
                        }
                );

        // TODO 获取 侧输出流
        // 获取到的是一个 DataStream，可以自行跟 主流 做一个关联，汇总结果
        resultDS.getSideOutput(late).print("side-output");

        env.execute();
    }
}
