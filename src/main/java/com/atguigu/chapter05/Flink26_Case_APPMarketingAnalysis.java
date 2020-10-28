package com.atguigu.chapter05;

import com.atguigu.bean.MarketingUserBehavior;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Arrays;
import java.util.List;
import java.util.Random;

/**
 * TODO
 *
 * @author cjp
 * @version 1.0
 * @date 2020/10/28 16:39
 */
public class Flink26_Case_APPMarketingAnalysis {
    public static void main(String[] args) throws Exception {
        // 0.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 1. 读取数据
        DataStreamSource<MarketingUserBehavior> appDS = env.addSource(new APPMarketingDataSource());

        // 2. 处理数据
        // 2.1 按照 统计维度（行为） 分组
        KeyedStream<Tuple2<String, Integer>, String> channelBehaviorAndOneKS = appDS
                .map(new MapFunction<MarketingUserBehavior, Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> map(MarketingUserBehavior value) throws Exception {
                        return Tuple2.of(value.getBehavior(), 1);
                    }
                })
                .keyBy(data -> data.f0);
        // 2.2 聚合
        SingleOutputStreamOperator<Tuple2<String, Integer>> resultDS = channelBehaviorAndOneKS.sum(1);

        // 3. 输出
        resultDS.print();

        //
        env.execute();
    }

    public static class APPMarketingDataSource implements SourceFunction<MarketingUserBehavior> {

        private static boolean flag = true;
        private List<String> behaviorList = Arrays.asList("DOWNLOAD", "INSTALL", "UPDATE", "UNINSTALL");
        private List<String> channelList = Arrays.asList("XIAOMI", "HUAWEI", "OPPO", "VIVO");


        @Override
        public void run(SourceContext<MarketingUserBehavior> ctx) throws Exception {
            Random random = new Random();
            while (flag) {
                ctx.collect(
                        new MarketingUserBehavior(
                                random.nextLong(),
                                behaviorList.get(random.nextInt(behaviorList.size())),
                                channelList.get(random.nextInt(channelList.size())),
                                System.currentTimeMillis())
                );
                Thread.sleep(1000L);
            }
        }

        @Override
        public void cancel() {
            flag = false;
        }
    }
}
