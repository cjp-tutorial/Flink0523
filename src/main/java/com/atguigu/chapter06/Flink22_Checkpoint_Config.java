package com.atguigu.chapter06;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.util.Collector;

import javax.annotation.Nullable;

/**
 *
 *
 * @author cjp
 * @version 1.0
 * @date 2020/10/30 14:34
 */
public class Flink22_Checkpoint_Config {

    public static void main(String[] args) throws Exception {
        // 0.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //TODO checkpoint
        // 1.算法名称： Chandy-Lamport 分布式快照算法
        //  生成一个barrier，插入到source前面，随着数据流的流动往下传递；
        //  每个task，接收到 barrier的时候，会触发 快照操作（保存到哪看 状态后端）
        //  每个task，完成快照操作后，会通知 JM
        //  当所有的task都完成了 快照，JM会通知：本次ck完成 => 2pc中的正式提交

        // 2. barrier对齐
        // 假设 source 并行度=2，下游的map并行度 = 1
        // map先接收到 source1的 bariier，会停止处理source1后续的数据，放在缓冲区中
        // 当 map接收到 source2（也就是所有的barrier）时，触发 map的 状态备份
        // 备份完成后，处理 缓冲区中的数据， 之后接着往下处理


        // 3. barrier不对齐
        // map先接收到 source1的 bariier，不停止处理source1后续的数据

        // barrier对齐才能 精准一次，不对齐只能 at-least-once

        //TODO checkpoint常用配置项
        env.enableCheckpointing(3000L); // 开启ck，设置周期
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(300000L);				// ck执行多久超时
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);				// 异步ck，同时有几个ck在执行
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500L);			// 上一个ck结束后，到下一个ck开启，最小间隔多久
        env.getCheckpointConfig().setPreferCheckpointForRecovery(false);		// 默认为 false，表示从 ck恢复；true，从savepoint恢复
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(3);		// 允许当前checkpoint失败的次数

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
        SingleOutputStreamOperator<String> resultDS = sensorDS
                .keyBy(sensor -> sensor.getId())
                .process(
                        new KeyedProcessFunction<String, WaterSensor, String>() {
                            // 1.定义状态；
                            ValueState<Integer> valueState;

                            @Override
                            public void open(Configuration parameters) throws Exception {
                                // 2. 在open中初始化状态
                                valueState = getRuntimeContext().getState(new ValueStateDescriptor<Integer>("value-state", Integer.class));
                            }

                            @Override
                            public void processElement(WaterSensor value, Context ctx, Collector<String> out) throws Exception {
                                out.collect("当前key=" + ctx.getCurrentKey() + ",值状态保存的上一次水位值=" + valueState.value());
                                // 保存上一条数据的水位值，保存到状态里
                                valueState.update(value.getVc());
                            }
                        }
                );

        resultDS.print();


        env.execute();
    }
}
