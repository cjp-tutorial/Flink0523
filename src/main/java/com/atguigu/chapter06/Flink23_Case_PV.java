package com.atguigu.chapter06;

import com.atguigu.bean.UserBehavior;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * TODO
 *
 * @author cjp
 * @version 1.0
 * @date 2020/10/28 15:26
 */
public class Flink23_Case_PV {
    public static void main(String[] args) throws Exception {
        // 0. 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // 1.读取数据
        SingleOutputStreamOperator<UserBehavior> userbehaviorDS = env
                .readTextFile("input/UserBehavior.csv")
                .map(new MapFunction<String, UserBehavior>() {
                    @Override
                    public UserBehavior map(String value) throws Exception {
                        String[] datas = value.split(",");
                        return new UserBehavior(
                                Long.valueOf(datas[0]),
                                Long.valueOf(datas[1]),
                                Integer.valueOf(datas[2]),
                                datas[3],
                                Long.valueOf(datas[4])
                        );
                    }
                })
                .assignTimestampsAndWatermarks(
                        new AscendingTimestampExtractor<UserBehavior>() {
                            @Override
                            public long extractAscendingTimestamp(UserBehavior element) {
                                return element.getTimestamp() * 1000L;
                            }
                        }
                );

        // 2.处理数据
        // 2.1 过滤 => 只保留 pv行为
        SingleOutputStreamOperator<UserBehavior> filterDS = userbehaviorDS.filter(data -> "pv".equals(data.getBehavior()));
        // 2.2 参考 wc 思路，实现统计
        SingleOutputStreamOperator<Tuple2<String, Integer>> pvAndOneDS = filterDS.map(new MapFunction<UserBehavior, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(UserBehavior value) throws Exception {
                return Tuple2.of("pv", 1);
            }
        });
        // 2.3 按照 pv 分组
        KeyedStream<Tuple2<String, Integer>, String> pvAndOneKS = pvAndOneDS.keyBy(data -> data.f0);
        // 2.4 聚合
        SingleOutputStreamOperator<Tuple2<String, Integer>> pvByWindowDS = pvAndOneKS
                .timeWindow(Time.hours(1))
                .sum(1);

        // 3.输出
        pvByWindowDS.print();

        // 4.执行
        env.execute();
    }
}
