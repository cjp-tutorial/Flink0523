package com.atguigu.chapter07;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;

/**
 * TODO
 *
 * @author cjp
 * @version 1.0
 * @date 2020/11/6 11:33
 */
public class Flink03_CEP_API {
    public static void main(String[] args) throws Exception {
        // 0.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // 1.读取数据
        SingleOutputStreamOperator<WaterSensor> sensorDS = env
                .readTextFile("input/sensor-data-cep.log")
                .map(new MapFunction<String, WaterSensor>() {
                    @Override
                    public WaterSensor map(String value) throws Exception {
                        String[] datas = value.split(",");
                        return new WaterSensor(datas[0], Long.valueOf(datas[1]), Integer.valueOf(datas[2]));
                    }
                })
                .assignTimestampsAndWatermarks(
//                        new AscendingTimestampExtractor<WaterSensor>() {
////                            @Override
////                            public long extractAscendingTimestamp(WaterSensor element) {
////                                return element.getTs() * 1000L;
////                            }
////                        }
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


        // 1.定义规则
        // times(n)  表示当前条件匹配N次（类似宽松近邻的关系）， 之后当作一个整体，再与其他事件产生关联（next、followedBy...）
        // times(m,n) 表示 m次到n次都可以，比如 1，3 => 1次、2次、3次都算
        // within(Time) 表示Time时间之内进行规则匹配，大于等于这个Time则超时
        Pattern<WaterSensor, WaterSensor> pattern = Pattern
                .<WaterSensor>begin("start")
                .where(new IterativeCondition<WaterSensor>() {
                    @Override
                    public boolean filter(WaterSensor value, Context<WaterSensor> ctx) throws Exception {
                        return "sensor_1".equals(value.getId());
                    }
                })
//                .times(1,3);
                .next("next")
                .where(new IterativeCondition<WaterSensor>() {
                    @Override
                    public boolean filter(WaterSensor value, Context<WaterSensor> ctx) throws Exception {
                        return "sensor_1".equals(value.getId());
                    }
                })
                .within(Time.seconds(3));
//                .times(2);


        // 2.应用规则
        PatternStream<WaterSensor> sensorPS = CEP.pattern(sensorDS, pattern);

        // 3.获取结果
        SingleOutputStreamOperator<String> resultDS = sensorPS.select(new PatternSelectFunction<WaterSensor, String>() {
            @Override
            public String select(Map<String, List<WaterSensor>> pattern) throws Exception {
//                WaterSensor start = pattern.get("start").iterator().next();
                return pattern.toString();
            }
        });


        resultDS.print();


        env.execute();
    }
}
