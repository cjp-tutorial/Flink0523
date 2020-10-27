package com.atguigu.chapter05;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;

import java.util.Properties;
import java.util.Random;

/**
 * TODO
 *
 * @author cjp
 * @version 1.0
 * @date 2020/10/27 14:34
 */
public class Flink05_Source_MySource {
    public static void main(String[] args) throws Exception {
        // 0.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 1.自定义数据源
        DataStreamSource<WaterSensor> inputDS = env.addSource(new MySourceFunction());

        inputDS.print();

        env.execute();
    }

    /**
     * 自定义Source
     * 1. 实现 Source Function，指定产生数据的类型
     * 2. 重写 run 和 cancel方法
     */
    public static class MySourceFunction implements SourceFunction<WaterSensor> {

        private boolean flag = true;

        @Override
        public void run(SourceContext<WaterSensor> ctx) throws Exception {

            Random random = new Random();

            while (flag) {
                WaterSensor waterSensor = new WaterSensor(
                        "sensor_" + random.nextInt(3),
                        System.currentTimeMillis(),
                        40 + random.nextInt(9)
                );
                ctx.collect(waterSensor);

                Thread.sleep(1000L);
            }
        }

        @Override
        public void cancel() {
            flag = false;
        }
    }
}
