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
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;

import java.util.List;
import java.util.Map;

/**
 * TODO
 *
 * @author cjp
 * @version 1.0
 * @date 2020/11/6 11:33
 */
public class Flink02_CEP_API {
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
                        new AscendingTimestampExtractor<WaterSensor>() {
                            @Override
                            public long extractAscendingTimestamp(WaterSensor element) {
                                return element.getTs() * 1000L;
                            }
                        }
                );

        // TODO 模式序列
        // next => 严格近邻 ，必须紧挨着，不能有小三插足
        // followedBy => 宽松近邻，不用紧挨着，可以有小三插足， 作为开始，匹配上就结束了，类似 一夫一妻
        // followedByAny => 非确定性宽松近邻，可以有小三插足，作为开始，有多少个就匹配多少个，全都要， 类似 一夫多妻
        // 1.定义规则
        Pattern<WaterSensor, WaterSensor> pattern = Pattern
                .<WaterSensor>begin("start")
                .where(new IterativeCondition<WaterSensor>() {
                    @Override
                    public boolean filter(WaterSensor value, Context<WaterSensor> ctx) throws Exception {
                        return "sensor_1".equals(value.getId());
                    }
                })
//                .next("next")
//                .followedBy("followedBy")
                .followedByAny("followedByAny")
                .where(new IterativeCondition<WaterSensor>() {
                    @Override
                    public boolean filter(WaterSensor value, Context<WaterSensor> ctx) throws Exception {
                        return "sensor_1".equals(value.getId());
                    }
                });

        // 2.应用规则
        PatternStream<WaterSensor> sensorPS = CEP.pattern(sensorDS, pattern);

        // 3.获取结果
        SingleOutputStreamOperator<String> resultDS = sensorPS.select(new PatternSelectFunction<WaterSensor, String>() {
            @Override
            public String select(Map<String, List<WaterSensor>> pattern) throws Exception {
                WaterSensor start = pattern.get("start").iterator().next();
//                WaterSensor next = pattern.get("next").iterator().next();
//                WaterSensor followedBy = pattern.get("followedBy").iterator().next();
                WaterSensor followedByAny = pattern.get("followedByAny").iterator().next();
//                return start + "-------->" + next;
//                return start + "-------->" + followedBy;
                return start + "-------->" + followedByAny;
            }
        });


        resultDS.print();


        env.execute();
    }
}
