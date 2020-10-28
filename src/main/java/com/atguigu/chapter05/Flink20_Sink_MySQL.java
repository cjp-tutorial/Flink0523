package com.atguigu.chapter05;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.HashMap;

/**
 * TODO
 *
 * @author cjp
 * @version 1.0
 * @date 2020/10/27 14:34
 */
public class Flink20_Sink_MySQL {
    public static void main(String[] args) throws Exception {
        // 0.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 1.读取数据
        SingleOutputStreamOperator<String> sensorDS = env
                .readTextFile("input/sensor-data.log");
//                .socketTextStream("localhost", 9999);

        // TODO ES Sink
        sensorDS.addSink(
                new MySQLSink()
        );

        env.execute();
    }

    public static class MySQLSink extends RichSinkFunction<String> {

        Connection conn = null;
        PreparedStatement pstmt = null;

        /**
         * 初始化 连接对象 和 预编译对象
         * @param parameters
         * @throws Exception
         */
        @Override
        public void open(Configuration parameters) throws Exception {
            conn = DriverManager.getConnection("jdbc:mysql://hadoop102:3306/test", "root", "000000");
            pstmt = conn.prepareStatement("INSERT INTO sensor VALUES (?,?,?)");
        }

        @Override
        public void close() throws Exception {
            pstmt.close();
            conn.close();
        }

        @Override
        public void invoke(String value, Context context) throws Exception {
            String[] sensorStrs = value.split(",");
            pstmt.setString(1, sensorStrs[0]);
            pstmt.setLong(2, Long.valueOf(sensorStrs[1]));
            pstmt.setInt(3, Integer.valueOf(sensorStrs[2]));

            pstmt.execute();
        }
    }

}
