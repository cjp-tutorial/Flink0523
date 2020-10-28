package com.atguigu.chapter05;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;

/**
 * TODO
 *
 * @author cjp
 * @version 1.0
 * @date 2020/10/27 14:34
 */
public class Flink18_Sink_Redis {
    public static void main(String[] args) throws Exception {
        // 0.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 1.读取数据
        SingleOutputStreamOperator<String> sensorDS = env
//                .readTextFile("input/sensor-data.log")
                .socketTextStream("localhost", 9999);

        // TODO redis Sink

        FlinkJedisPoolConfig conf = new FlinkJedisPoolConfig.Builder()
                .setHost("hadoop102")
                .setPort(6379)
                .build();

        sensorDS.addSink(
                new RedisSink<>(conf,
                        new RedisMapper<String>() {

                            /**
                             * 指定 redis 的操作命令，和最外层的key
                             * @return
                             */
                            @Override
                            public RedisCommandDescription getCommandDescription() {
                                return new RedisCommandDescription(RedisCommand.HSET,"sensor0523");
                            }

                            /**
                             * 从数据中获取 key：在Hash中，就是 Hash的key
                             * @param data
                             * @return
                             */
                            @Override
                            public String getKeyFromData(String data) {
                                return data.split(",")[1];

                            }

                            /**
                             * 从数据中获取 value：在Hash中，就是 Hash的value
                             * @param data
                             * @return
                             */
                            @Override
                            public String getValueFromData(String data) {
                                return data.split(",")[2];
                            }
                        })
        );



        env.execute();
    }

}
