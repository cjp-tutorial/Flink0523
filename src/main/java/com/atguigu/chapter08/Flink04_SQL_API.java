package com.atguigu.chapter08;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.OldCsv;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.types.Row;

/**
 * TODO
 *
 * @author cjp
 * @version 1.0
 * @date 2020/11/7 9:23
 */
public class Flink04_SQL_API {
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

        // TODO SQL 基本使用
        // 1.创建 表的执行环境,不管是 TableAPI还是SQL，都需要先做这一步
//        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .inStreamingMode()
                .useOldPlanner()
                .build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

        // 2. 把 DataStream 转成一个 Table
        Table table = tableEnv.fromDataStream(sensorDS, "id,vc,ts as timeeeeeees");
        tableEnv.createTemporaryView("sensorTable", table);
//        tableEnv.createTemporaryView("sensorTable",sensorDS , "id,vc,ts as timeeeeeees");

        // TODO 3. 使用 SQL 对 Table进行操作
//        Table resultTable = tableEnv.sqlQuery("select * from " + table);
        Table resultTable = tableEnv.sqlQuery("select id,vc,timeeeeeees from sensorTable");

       // 使用 connect 关联外部系统，抽象成一张表
        tableEnv.connect(new FileSystem().path("output/flink1.txt"))
                .withFormat(new OldCsv().fieldDelimiter("-"))
                .withSchema(
                        new Schema()
                        .field("asasdf", DataTypes.STRING())
                        .field("vvvvcccc", DataTypes.INT())
                        .field("tttttsssss",DataTypes.BIGINT() )
                )
                .createTemporaryTable("heiheihei");


        Table heiheihei = tableEnv.from("heiheihei");

        // 使用 SQL 对 外部系统抽象的表 进行插入
        tableEnv.sqlUpdate("INSERT INTO heiheihei SELECT * FROM sensorTable");

        env.execute();
    }
}
