package com.atguigu.chapter08;

import com.atguigu.bean.UserBehavior;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.shaded.guava18.com.google.common.hash.BloomFilter;
import org.apache.flink.shaded.guava18.com.google.common.hash.Funnels;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
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
public class Flink11_Case_UVWithBloomFilter {
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

        // 2.3 分组、开窗
        uvAndUserIdDS.keyBy(data -> data.f0)
                .timeWindow(Time.hours(1))
                .aggregate(new AggWithBloomFilter(), new MyProcessWindowFunction())
                .print();


        // 4.执行
        env.execute();
    }

    public static class AggWithBloomFilter implements AggregateFunction<Tuple2<String, Long>, Tuple2<Long, BloomFilter<Long>>, Long> {

        @Override
        public Tuple2<Long, BloomFilter<Long>> createAccumulator() {
            // 创建布隆过滤器
            // 第一个参数，指定类型
            // 第二个参数，预计的数据量 => 根据数据量推算出格子数
            // 第三个参数，错误率，默认是0.03，可以指定
            BloomFilter<Long> longBloomFilter = BloomFilter.create(Funnels.longFunnel(), 1000000, 0.01);
            return Tuple2.of(0L, longBloomFilter);
        }

        @Override
        public Tuple2<Long, BloomFilter<Long>> add(Tuple2<String, Long> value, Tuple2<Long, BloomFilter<Long>> accumulator) {
            Long userId = value.f1;
            Long uvCount = accumulator.f0;
            BloomFilter<Long> bloomFilter = accumulator.f1;
            // 判断 布隆过滤器 里 是否已经有 这个 userId
            if (!bloomFilter.mightContain(userId)) {
                // 布隆里不包含该 userId => 1. count + 1； 2. 把 userId放入 布隆
                uvCount++;
                bloomFilter.put(userId);
            }
            return Tuple2.of(uvCount, bloomFilter);
        }

        @Override
        public Long getResult(Tuple2<Long, BloomFilter<Long>> accumulator) {
            return accumulator.f0;
        }

        @Override
        public Tuple2<Long, BloomFilter<Long>> merge(Tuple2<Long, BloomFilter<Long>> a, Tuple2<Long, BloomFilter<Long>> b) {
            return null;
        }
    }

    public static class MyProcessWindowFunction extends ProcessWindowFunction<Long,String,String,TimeWindow>{

        @Override
        public void process(String s, Context context, Iterable<Long> elements, Collector<String> out) throws Exception {
            Long uvCount = elements.iterator().next();
            out.collect("窗口["+context.window().getStart()+","+context.window().getEnd()+")的uv值为="+uvCount);
        }
    }
}
