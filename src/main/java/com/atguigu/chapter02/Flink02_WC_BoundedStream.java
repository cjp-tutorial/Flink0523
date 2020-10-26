package com.atguigu.chapter02;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
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
 * @date 2020/10/26 10:33
 */
public class Flink02_WC_BoundedStream {
    public static void main(String[] args) throws Exception {
        // 0.创建 流式的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 1.读取数据: 有界流 => 有头有尾 => 文件
//        DataStreamSource<String> fileDS = env.readTextFile("input/word.txt");
        // 虚拟机上的路径
        DataStreamSource<String> fileDS = env.readTextFile("/opt/module/data/word.txt");

        // 2.处理数据
        // 2.1 压平操作 => 切分、转换成（word，1）二元组
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordAndOneDS = fileDS.flatMap(
                (value, out) -> {
                    String[] words = value.split(" ");
                    for (String word : words) {
                        Tuple2<String, Integer> tuple2 = Tuple2.of(word, 1);
                        // 使用采集器，往下游发送数据
                        out.collect(tuple2);
                    }
                }
        );

        // 扩展：
        // 使用lambda表达式，flink不认识Tuple，需要用returns明确指定
        // Flink内部对类型封装成 TypeInformation
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordAndOneReturns = wordAndOneDS.returns(new TypeHint<Tuple2<String, Integer>>() {
        });

        // 2.2 按照 word 分组
        KeyedStream<Tuple2<String, Integer>, Tuple> wordAndOneKS = wordAndOneReturns.keyBy(0);
        // 2.3 聚合
        SingleOutputStreamOperator<Tuple2<String, Integer>> resultDS = wordAndOneKS.sum(1);

        // 3.输出
        resultDS.print();

        // 4.启动
        env.execute();
    }
}
