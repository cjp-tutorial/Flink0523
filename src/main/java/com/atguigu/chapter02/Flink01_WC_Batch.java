package com.atguigu.chapter02;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;

/**
 * TODO
 *
 * @author cjp
 * @version 1.0
 * @date 2020/10/26 10:08
 */
public class Flink01_WC_Batch {
    public static void main(String[] args) throws Exception {

        // 创建上下文
        // 0.创建 执行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // 1.读取数据
        DataSource<String> fileDS = env.readTextFile("input/word.txt");

        // 2.处理数据
        // 2.1 压平操作 =》 切分、转化成（word，1）的二元组
        FlatMapOperator<String, Tuple2<String, Integer>> tuple2DS = fileDS.flatMap(new MyFlatMapFunction());

        // 2.2 分组 =》 按照 word 分组
        UnsortedGrouping<Tuple2<String, Integer>> wordAndOneGroup = tuple2DS.groupBy(0);

        // 2.3 聚合 =>
        AggregateOperator<Tuple2<String, Integer>> resultDS = wordAndOneGroup.sum(1);

        // 输出
        resultDS.print();

        // 启动(批处理不用启动)
//        env.execute("ddd");

    }

    public static class MyFlatMapFunction implements FlatMapFunction<String, Tuple2<String,Integer>>{

        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
            String[] words = value.split(" ");
            for (String word : words) {
                Tuple2<String, Integer> tuple2 = new Tuple2<>(word, 1);
                // 使用采集器，收集数据，往下游发送
                out.collect(tuple2);
            }
        }
    }
}
