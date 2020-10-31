package com.atguigu.chapter06;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.util.Collector;

import javax.annotation.Nullable;

/**
 * 连续5s水位上涨 告警
 *
 * @author cjp
 * @version 1.0
 * @date 2020/10/30 14:34
 */
public class Flink16_ProcessFunction_TimerServicePractice {
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
        sensorDS
                .keyBy(sensor -> sensor.getId())
                .process(
                        new KeyedProcessFunction<String, WaterSensor, String>() {

                            private Integer lastVC = -1;
                            private Long timerTs = 0L;

                            @Override
                            public void processElement(WaterSensor value, Context ctx, Collector<String> out) throws Exception {
                                // 判断水位是否上升
                                if (value.getVc() > lastVC) {
                                    // 1.水位上升
                                    // 第一条数据要注册定时器， 第一条数据的 vc 肯定大于 lastVC的默认值 -1
                                    // 注册一个5s后的定时器
                                    if (timerTs == 0) {
                                        timerTs = ctx.timestamp() + 5000L;
                                        ctx.timerService().registerEventTimeTimer(timerTs);
                                    }
                                } else {
                                    // 2.水位下降 => 不满足5s内连续上升的要求，删除定时器
                                    ctx.timerService().deleteEventTimeTimer(timerTs);
                                    // 清空保存的 定时器注册时间 =》 避免影响后续的数据 注册定时器
                                    timerTs = 0L;
                                }
                                // 不管上升还是下降，水位值都要保存更新
                                lastVC = value.getVc();
                            }

                            /**
                             * 定时器触发：说明 已经 5s连续上涨了
                             * @param timestamp
                             * @param ctx
                             * @param out
                             * @throws Exception
                             */
                            @Override
                            public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
                                out.collect("传感器："+ctx.getCurrentKey()+"监控到水位5s内连续上涨！");
                                // 通常，在定时器触发之后，要考虑 重置一些变量的状态
                                timerTs = 0L;
                            }
                        }
                )
                .print();


        env.execute();
    }
}
