package com.atguigu.chapter06;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import javax.annotation.Nullable;

/**
 * TODO
 *
 * @author cjp
 * @version 1.0
 * @date 2020/10/30 14:34
 */
public class Flink09_Watermark_BoundedStreanIssue {
    public static void main(String[] args) throws Exception {
        // 0.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // TODO 有界流 关于 watermark的问题
        // 为了保证所有的数据，都能被计算，所以，会在结束之前，把 watermark设置为 Long的最大值
        // 因为升序的watermark是周期性的200ms，读取完数据，还没达到200ms，也就是watermark还来不及更新，就处理完毕
        //      => 所以看起来，所有窗口同时被 Long的最大值 触发


        // 1.读取数据
        SingleOutputStreamOperator<WaterSensor> sensorDS = env
                .readTextFile("input/sensor-data.log")
//                .socketTextStream("localhost",9999 )
                .map(new MapFunction<String, WaterSensor>() {
                    @Override
                    public WaterSensor map(String value) throws Exception {
                        String[] datas = value.split(",");
                        return new WaterSensor(datas[0], Long.valueOf(datas[1]), Integer.valueOf(datas[2]));
                    }
                })
                .assignTimestampsAndWatermarks(
                        new AscendingTimestampExtractor<WaterSensor>() {
                            @Override
                            public long extractAscendingTimestamp(WaterSensor element) {
                                return element.getTs() * 1000L;
                            }
                        }
//                        new AssignerWithPunctuatedWatermarks<WaterSensor>() {
//
//                            @Override
//                            public long extractTimestamp(WaterSensor element, long previousElementTimestamp) {
//                                return element.getTs() * 1000L;
//                            }
//
//                            @Nullable
//                            @Override
//                            public Watermark checkAndGetNextWatermark(WaterSensor lastElement, long extractedTimestamp) {
//                                return new Watermark(extractedTimestamp);
//                            }
//                        }
                );

        // 2.处理数据
        // 分组、开窗、全窗口函数
        sensorDS
                .keyBy(sensor -> sensor.getId())
                .timeWindow(Time.seconds(5))
                .process(
                        new ProcessWindowFunction<WaterSensor, String, String, TimeWindow>() {

                            @Override
                            public void process(String s, Context context, Iterable<WaterSensor> elements, Collector<String> out) throws Exception {
                                System.out.println("process...");
                                out.collect("当前key=" + s
                                        + "当前watermark=" + context.currentWatermark()
                                        + "窗口为[" + context.window().getStart() + "," + context.window().getEnd() + ")"
                                        + ",一共有" + elements.spliterator().estimateSize() + "条数据");
                            }
                        }
                )
                .print();

        env.execute();
    }
}
