package com.atguigu.chapter05;

import com.atguigu.bean.UserBehavior;
import org.apache.flink.api.common.accumulators.Accumulator;
import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

/**
 * TODO
 *
 * @author cjp
 * @version 1.0
 * @date 2020/10/28 15:26
 */
public class Flink22_Case_PVByProcess {
    public static void main(String[] args) throws Exception {
        // 0. 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

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
                });

        // 2.处理数据
        // 2.1 过滤
        SingleOutputStreamOperator<UserBehavior> filterDS = userbehaviorDS.filter(data -> "pv".equals(data.getBehavior()));

/*        filterDS.process(
                new ProcessFunction<UserBehavior, Long>() {

                    @Override
                    public void processElement(UserBehavior value, Context ctx, Collector<Long> out) throws Exception {

                    }
                }
        )*/

        // 2.2 按照 pv行为 分组
        KeyedStream<UserBehavior, String> userBehaviorKS = filterDS.keyBy(data -> data.getBehavior());
        // 2.3 使用 process 进行 计数统计
        userBehaviorKS
                .process(
                        new KeyedProcessFunction<String, UserBehavior, Long>() {
                            private long pvCount = 0;

                            @Override
                            public void processElement(UserBehavior value, Context ctx, Collector<Long> out) throws Exception {
                                pvCount++;
                                out.collect(pvCount);
                            }
                        }
                )
                .print();


        // 4.执行
        env.execute();
    }
}
