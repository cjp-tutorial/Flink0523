package com.atguigu.chapter08;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
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
public class Flink03_TableAPI_API {
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

        // TODO TableAPI基本使用
        // 1.创建 表的执行环境,不管是 TableAPI还是SQL，都需要先做这一步
//        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .inStreamingMode()
                .useOldPlanner()
                .build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

        // 2. 把 DataStream 转成一个 Table
        Table table = tableEnv.fromDataStream(sensorDS, "id,vc,ts as timeeeeeees");

        // 3. 对 Table进行操作
        Table resultTable = table
                .where("id = 'sensor_1'")
                .select("id,timeeeeeees,vc");

        // 4. 定义一个 外部系统的抽象 Table
        // TODO 把 外部系统 抽象成 一个 Table对象,那么就变成了 Table与 Table之间的操作
        //    首先，通过connect与外部系统产生关联
        //    然后，通过 withFormar指定数据在 外部系统里的存储格式
        //    接着，通过 Schema指定 抽象成的 Table的 表结构信息 => 字段名和类型
        //    最后，通过 createTemporaryTable 给 抽象的Table 起一个 表名
        //  =》 可以作为 Sink，也可以作为 Source
        tableEnv.connect(new FileSystem().path("output/flink.txt"))
                .withFormat(new OldCsv().fieldDelimiter("|"))
                .withSchema(
                        new Schema()
                                .field("aaa", DataTypes.STRING())
                                .field("bbb", DataTypes.BIGINT())
                                .field("ccc", DataTypes.INT())
                )
                .createTemporaryTable("hahaha");

        // 5.保存数据
        resultTable.insertInto("hahaha");


        env.execute();
    }
}
