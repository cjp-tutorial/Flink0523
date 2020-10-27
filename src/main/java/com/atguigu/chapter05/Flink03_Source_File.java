package com.atguigu.chapter05;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;

/**
 * TODO
 *
 * @author cjp
 * @version 1.0
 * @date 2020/10/27 14:27
 */
public class Flink03_Source_File {
    public static void main(String[] args) throws Exception {
        // 0.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 1.从文件读取数据
        DataStreamSource<String> fileDS = env.readTextFile("input/sensor-data.log");

        // 更灵活的读取文件，可以自定义输入的文件格式
//        env.readFile(, );

        //
        fileDS.print();

        env.execute();
    }
}
