package com.atguigu.chapter08;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.Tumble;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * TODO
 *
 * @author cjp
 * @version 1.0
 * @date 2020/11/7 9:23
 */
public class Flink08_GroupWindow {
    public static void main(String[] args) throws Exception {
        // 0.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // 1.读取数据
        SingleOutputStreamOperator<WaterSensor> sensorDS = env
                .readTextFile("input/sensor-data.log")
                .map(new MapFunction<String, WaterSensor>() {
                    @Override
                    public WaterSensor map(String value) throws Exception {
                        String[] datas = value.split(",");
                        return new WaterSensor(datas[0], Long.valueOf(datas[1]), Integer.valueOf(datas[2]));
                    }
                })
                .assignTimestampsAndWatermarks(
                        new AscendingTimestampExtractor<WaterSensor>() {
                            @Override
                            public long extractAscendingTimestamp(WaterSensor element) {
                                return element.getTs() * 1000L;
                            }
                        }
                );


        // 1.创建 表的执行环境,不管是 TableAPI还是SQL，都需要先做这一步
        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .inStreamingMode()
                .useOldPlanner()
                .build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

        // 2. 把 DataStream 转成一个 Table
        tableEnv.createTemporaryView("sensorTable", sensorDS, "id,vc,ts.rowtime as rt");
//        tableEnv.createTemporaryView("sensorTable", sensorDS, "id,vc,ts.proctime as rt");
        Table sensorTable = tableEnv.from("sensorTable");


        // TODO 使用 TableAPI实现 GroupWindow
        // 1. 用 .window来指定窗口
        //      => 首先，指定窗口类型， Tumble、Slide、Session
        //      => 然后，指定窗口参数， over("时间（行数）.时间单位复数（或rows）")
        //      => 接着，指定 用来分组（按时间间隔）或者排序（按行数）的时间字段 =》字段的指定，用 时间字段.rowtime 或 时间字段.proctime，可以起 别名
        //      => 最后，指定 窗口的别名， as("别名")
        // 2. 将 窗口 放在 分组字段里，也就是放在 groupby 里
/*        Table resultTable = sensorTable
                .window(Tumble.over("5.seconds").on("rt").as("w"))
                .groupBy("id,w")
                .select("id,count(id) as cnt,w.start,w.end");*/

        Table resultTable = tableEnv.sqlQuery("select " +
                "id," +
                "count(id) as cnt," +
                "HOP_START(rt,INTERVAL '2' SECOND,INTERVAL '5' SECOND) as window_start," +
                "HOP_END(rt,INTERVAL '2' SECOND,INTERVAL '5' SECOND) as window_end " +
                "from sensorTable " +
                "group by id,HOP(rt,INTERVAL '2' SECOND,INTERVAL '5' SECOND)");


        tableEnv.toRetractStream(resultTable, Row.class).print();

        // TODO 使用 SQL 实现 GroupWindow



        env.execute();
    }
}
