package com.atguigu.chapter05;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * TODO
 *
 * @author cjp
 * @version 1.0
 * @date 2020/10/27 14:14
 */
public class Flink01_Environment {
    public static void main(String[] args) {

        // Spark
        // idea运行： 要加 .setMaster("local[*]")
        // 提交到集群：


        ExecutionEnvironment benv = ExecutionEnvironment.getExecutionEnvironment();
//        ExecutionEnvironment.createLocalEnvironment();
//        ExecutionEnvironment.createRemoteEnvironment(, );


        StreamExecutionEnvironment senv = StreamExecutionEnvironment.getExecutionEnvironment();
//        StreamExecutionEnvironment.createLocalEnvironment();
//        StreamExecutionEnvironment.createRemoteEnvironment(, );
    }
}
