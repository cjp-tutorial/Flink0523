package com.atguigu.chapter08;

import com.atguigu.bean.UserBehavior;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Slide;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * 实时热门商品统计 -- 每隔5分钟 输出 最近一小时内 点击量最多的 前N个商品
 *
 * @author cjp
 * @version 1.0
 * @date 2020/10/28 15:26
 */
public class Flink10_Case_HotItemAnalysis {
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

        // TODO 2.使用 FlinkSQL 处理数据：每隔5分钟 输出 最近一小时内 点击量最多的 前N个商品
        // 0. 创建 表的 执行环境
        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .inStreamingMode()
                .useBlinkPlanner()  // 只有 Blink支持 TopN语法
                .build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);
        // 1. 使用 TableAPI 进行开窗
        Table userBehavioTable = tableEnv.fromDataStream(userbehaviorDS, "itemId,behavior,timestamp.rowtime as rt");
        Table aggTable = userBehavioTable
                .where("behavior == 'pv'")
                .window(Slide.over("1.hours").every("5.minutes").on("rt").as("w"))
                .groupBy("itemId,w")
                .select("itemId,count(itemId) as itemCount,w.end as windowEnd");

        DataStream<Row> aggDS = tableEnv.toAppendStream(aggTable, Row.class);
        tableEnv.createTemporaryView("aggTable", aggDS, "itemId,itemCount,windowEnd");

        // 2. 使用 SQL 进行 TopN 的实现
        Table resultTable = tableEnv.sqlQuery("select * " +
                "from (" +
                "select " +
                "*, " +
                "row_number() over(partition by windowEnd order by itemCount desc) as rk " +
                "from aggTable) " +
                "where rk <= 3");

        tableEnv.toRetractStream(resultTable, Row.class).print();

        // 4.执行
        env.execute();
    }
}
