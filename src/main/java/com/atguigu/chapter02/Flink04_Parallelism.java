package com.atguigu.chapter02;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * TODO
 *
 * @author cjp
 * @version 1.0
 * @date 2020/10/26 10:42
 */
public class Flink04_Parallelism {
    public static void main(String[] args) throws Exception {
        // 0.创建 流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // TODO 在 执行环境中 设置并行度 => 全局设置
//        env.setParallelism(2);

        // TODO 全局禁用 操作链
        env.disableOperatorChaining();

        // TODO 并行度设置的优先级
        // 算子（代码） > 全局（代码） > 提交参数 > 配置文件

        // TODO 并行度与slot的关系
        // 1.Flink的并行度可以对算子进行设置，那么算子的子任务的数量，就是算子的并行度
        // 2.Job的并行度是多少？ => 等于 并行度最大的算子 的 并行度
        // 3.一个Job需要多少个slot？ => 取决于 Job的并行度

        // TODO Task 与 subtask
        // 算子的一个并行子任务，叫做 subtask
        // task 是由 不同算子的 subtask 根据一定的规则 合并在一起形成

        // TODO OperatorChains => 把 不同算子的 子任务 合并起来的  规则
        // 满足 One to One的关系，并且并行度相同

        // 1.读取数据 => 无界流：kafka、socket
//        DataStreamSource<String> socketDS = env.socketTextStream("localhost", 9999);
        DataStreamSource<String> socketDS = env.socketTextStream("hadoop102", 9999);

        // 2.处理数据
        // 2.1 扁平化操作
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordAndOneDS = socketDS.flatMap(
                new FlatMapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                        String[] words = value.split(" ");
                        for (String word : words) {
                            Tuple2<String, Integer> tuple2 = Tuple2.of(word, 1);
                            // 采集器
                            out.collect(tuple2);
                        }
                    }
                }
        );

//                .startNewChain();   // 从当前算子开始，开启一个新的 操作链
//                .disableChaining();   // 当前算子禁用操作链，当前算子不会被加入操作链，包括前面和后面

        SingleOutputStreamOperator<Tuple2<String, Integer>> map = wordAndOneDS.map(new MapFunction<Tuple2<String, Integer>, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(Tuple2<String, Integer> value) throws Exception {
                return value;
            }
        });

        // 2.2 分组
        KeyedStream<Tuple2<String, Integer>, Tuple> wordAndOneKS = map.keyBy(0);
        // 2.3 聚合
        SingleOutputStreamOperator<Tuple2<String, Integer>> resultDS = wordAndOneKS.sum(1);

        // 3.输出
        resultDS.print();

        // 4.执行
        env.execute();

    }
}
