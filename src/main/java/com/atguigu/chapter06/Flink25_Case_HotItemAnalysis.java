package com.atguigu.chapter06;

import com.atguigu.bean.ItemCountWithWindowEnd;
import com.atguigu.bean.UserBehavior;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.*;

/**
 * 实时热门商品统计 -- 每隔5分钟 输出 最近一小时内 点击量最多的 前N个商品
 *
 * @author cjp
 * @version 1.0
 * @date 2020/10/28 15:26
 */
public class Flink25_Case_HotItemAnalysis {
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
        // 2.1 过滤 => 只保留 pv 行为
        SingleOutputStreamOperator<UserBehavior> filterDS = userbehaviorDS.filter(data -> "pv".equals(data.getBehavior()));
        // 2.2 按照 统计维度 分组： 商品
        KeyedStream<UserBehavior, Long> userBehaviorKS = filterDS.keyBy(data -> data.getItemId());
        // 2.3 开窗 - 每隔5分钟 输出 最近一小时内 点击量最多的 前N个商品 => 滑动窗口： 长度 1小时、步长 5分钟
        WindowedStream<UserBehavior, Long, TimeWindow> userBehaviorWS = userBehaviorKS.timeWindow(Time.hours(1), Time.minutes(5));
        // TODO 窗口内 求和、排序、取前N个
        // 2.4 求和?
        // sum ? => 转成 （商品ID，1） => 得到 （1001，10），（1002，20）
//        userBehaviorWS.sum()
//        userBehaviorWS.reduce()
        // 全窗口、攒数据，不太好
//        userBehaviorWS.process()

        // aggregate传两个参数：
        //     => 第一个参数：AggregateFunction ，增量的聚合函数， 输出 给 第二个参数作为输入
        //     => 第二个参数：全窗口函数， 它的输入就是 与聚合函数的 输出
        // 假设有如下商品
        //  1001
        //  1001
        //  1001
        //  1001
        //  1002
        //  1002
        // => 预聚合函数，得到 4、2 两条结果数据
        // => 预聚合函数的结果 输出给  全窗口函数 （不需要在数据中指明分组的key(商品ID)，这个key在上下文可以获取）
        // => 中小电商，商品大概有 几万 到 小几十万， 所以 预聚合的结果，大概也只有 几万到小几十万，而且每一条都只是一个 统计的数字
        SingleOutputStreamOperator<ItemCountWithWindowEnd> aggWithWindowEndDS = userBehaviorWS.aggregate(new AggFunction(), new CountByWindowEnd());

        // 2.5 按照 窗口结束时间 分组 => 为了让 同一个窗口的统计结果 放到同一个分组，方便后续进行排序
        KeyedStream<ItemCountWithWindowEnd, Long> aggKS = aggWithWindowEndDS.keyBy(data -> data.getWindowEnd());

        // 2.6 使用 process 进行排序
        aggKS
                .process(new TopNProcessFunction(3))
                .print();

        // 4.执行
        env.execute();
    }


    /**
     * 预聚合函数: 它的输出 是 全窗口函数的 输入
     */
    public static class AggFunction implements AggregateFunction<UserBehavior, Long, Long> {

        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(UserBehavior value, Long accumulator) {
            return accumulator + 1L;
        }

        @Override
        public Long getResult(Long accumulator) {
            return accumulator;
        }

        @Override
        public Long merge(Long a, Long b) {
            return a + b;
        }
    }

    /**
     * 全窗口函数： 把预聚合的结果，打上 窗口结束时间 的标签
     * => 输入类型 就是 预聚合函数的 输出类型
     * => 为了后续 按照窗口结束时间分组，让同一个窗口的统计结果在一起，进行排序
     */
    public static class CountByWindowEnd extends ProcessWindowFunction<Long, ItemCountWithWindowEnd, Long, TimeWindow> {

        /**
         * 进入这个方法，是 同一个窗口的 同一个分组的 全部数据
         * => 对于 1001 这个商品（分组）来说，已经是一个预聚合的结果， 只有一条 4
         *
         * @param itemId
         * @param context
         * @param elements
         * @param out
         * @throws Exception
         */
        @Override
        public void process(Long itemId, Context context, Iterable<Long> elements, Collector<ItemCountWithWindowEnd> out) throws Exception {
            out.collect(new ItemCountWithWindowEnd(itemId, elements.iterator().next(), context.window().getEnd()));
        }
    }


    /**
     *
     */
    public static class TopNProcessFunction extends KeyedProcessFunction<Long, ItemCountWithWindowEnd, String> {

        ListState<ItemCountWithWindowEnd> datas;
        ValueState<Long> triggerTs;

        private int threshold;
        private int currentThreshold;

        public TopNProcessFunction(int threshold) {
            this.threshold = threshold;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            datas = getRuntimeContext().getListState(new ListStateDescriptor<ItemCountWithWindowEnd>("datas", ItemCountWithWindowEnd.class));
            triggerTs = getRuntimeContext().getState(new ValueStateDescriptor<Long>("triggerTs", Long.class));
        }

        @Override
        public void processElement(ItemCountWithWindowEnd value, Context ctx, Collector<String> out) throws Exception {
            // 排序 =>
            // 数据是一条一条处理的，所以先把数据存起来
            datas.add(value);
            // 存到啥时候？ => 等本窗口的所有数据到齐 => 模拟窗口的触发 => 定时器 =》 预留点时间
            if (triggerTs.value() == null) {
                ctx.timerService().registerEventTimeTimer(value.getWindowEnd() + 10L);
                triggerTs.update(value.getWindowEnd() + 10L);
            }
        }

        /**
         * 定时器触发：说明 同一窗口的 统计结果 已经到齐了，进行排序、取前N个
         *
         * @param timestamp
         * @param ctx
         * @param out
         * @throws Exception
         */
        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            Iterable<ItemCountWithWindowEnd> datasIt = datas.get();
            // 创建一个 List，用来 存 数据，进行排序
            List<ItemCountWithWindowEnd> itemCountWithWindowEnds = new ArrayList<>();
            for (ItemCountWithWindowEnd itemCountWithWindowEnd : datasIt) {
                itemCountWithWindowEnds.add(itemCountWithWindowEnd);
            }
            // 过河拆桥, 清空保存的 数据和时间
            datas.clear();
            triggerTs.clear();

            // 排序
//            itemCountWithWindowEnds.sort(
//                    new Comparator<ItemCountWithWindowEnd>() {
//                        @Override
//                        public int compare(ItemCountWithWindowEnd o1, ItemCountWithWindowEnd o2) {
//                            // 后 减 前 -> 降序，这里要降序；
//                            // 前 减 后 -> 升序
//                            return o2.getItemCount().intValue() - o1.getItemCount().intValue();
//                        }
//                    }
//            );

            Collections.sort(itemCountWithWindowEnds);

            // 取前N个
            StringBuilder resultStr = new StringBuilder();
            resultStr.append("-------------------------------------------------------\n");
            resultStr.append("窗口结束时间:" + (timestamp - 10L) + "\n");

            // 判断一下 传参 与 实际个数的 大小，防止 越界问题
            currentThreshold = threshold > itemCountWithWindowEnds.size() ? itemCountWithWindowEnds.size() : threshold;
            for (int i = 0; i < currentThreshold; i++) {
                resultStr.append("Top" + (i + 1) + ":" + itemCountWithWindowEnds.get(i) + "\n");
            }
            resultStr.append("-------------------------------------------------------\n\n\n");
            out.collect(resultStr.toString());
        }
    }
}
