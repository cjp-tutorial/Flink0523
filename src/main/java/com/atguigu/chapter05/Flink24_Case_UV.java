package com.atguigu.chapter05;

import com.atguigu.bean.UserBehavior;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.HashSet;
import java.util.Set;

/**
 * TODO
 *
 * @author cjp
 * @version 1.0
 * @date 2020/10/28 15:26
 */
public class Flink24_Case_UV {
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
        // 2.2 转换成 （"uv"，用户ID）格式
        // => uv是为了分组用
        // => 用户ID，是为了放入Set去重， 其他的字段不关心，不需要
        SingleOutputStreamOperator<Tuple2<String, Long>> uvAndUserIdDS = filterDS.map(
                new MapFunction<UserBehavior, Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> map(UserBehavior value) throws Exception {
                        return Tuple2.of("uv", value.getUserId());
                    }
                }
        );

        // 2.3 按照 uv 分组
        KeyedStream<Tuple2<String, Long>, String> uvAndUserIdKS = uvAndUserIdDS.keyBy(data -> data.f0);
        // 2.4 
        SingleOutputStreamOperator<Long> uvDS = uvAndUserIdKS.process(
                new KeyedProcessFunction<String, Tuple2<String, Long>, Long>() {
                    // 定义一个Set，用来存储 userID
                    private Set<Long> userIdSet = new HashSet<>();

                    @Override
                    public void processElement(Tuple2<String, Long> value, Context ctx, Collector<Long> out) throws Exception {
                        // 每来一条数据，就把 userID 放入 Set中
                        userIdSet.add(value.f1);
                        out.collect(Long.valueOf(userIdSet.size()));
                    }
                }
        );

        // 3.输出
        uvDS.print();

        // 4.执行
        env.execute();
    }
}
