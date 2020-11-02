package com.atguigu.chapter06;

import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

/**
 * TODO
 *
 * @author cjp
 * @version 1.0
 * @date 2020/11/2 9:11
 */
public class Flink20_State_BroadcastState {
    public static void main(String[] args) throws Exception {
        // 0.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 1.读取数据
        DataStreamSource<String> inputDS = env.socketTextStream("localhost", 9999);
        DataStreamSource<String> controlDS = env.socketTextStream("localhost", 8888);

        // TODO 1.把其中一条流（规则、配置文件，数据量比较小的、更新比较慢的） 广播
        MapStateDescriptor<String, String> broadcastStateDesc = new MapStateDescriptor<>("broadcast-state", String.class, String.class);
        BroadcastStream<String> controlBS = controlDS.broadcast(broadcastStateDesc);

        // TODO 2.将 主流A 与 广播流 连接起来
        BroadcastConnectedStream<String, String> dataBCS = inputDS.connect(controlBS);

        // TODO 3.
        dataBCS.process(
                new BroadcastProcessFunction<String, String, String>() {
                    /**
                     * 主流里的 数据 的处理
                     * @param value
                     * @param ctx
                     * @param out
                     * @throws Exception
                     */
                    @Override
                    public void processElement(String value, ReadOnlyContext ctx, Collector<String> out) throws Exception {
                        // 主流，只能获取使用 广播状态（只读），不能对其进行修改
                        ReadOnlyBroadcastState<String, String> broadcastState = ctx.getBroadcastState(broadcastStateDesc);
                        out.collect("主流的数据=" + value + ",广播状态的值=" + broadcastState.get("switch"));
                    }

                    /**
                     * 广播流里面的 数据的处理，定义广播状态里放什么数据
                     * @param value
                     * @param ctx
                     * @param out
                     * @throws Exception
                     */
                    @Override
                    public void processBroadcastElement(String value, Context ctx, Collector<String> out) throws Exception {
                        BroadcastState<String, String> broadcastState = ctx.getBroadcastState(broadcastStateDesc);
                        String key = "switch";
                        if ("1".equals(value)) {
                            broadcastState.put(key, "true");
                        } else {
                            broadcastState.put(key, "false");
                        }
                    }
                }
        )
                .print();


        env.execute();
    }
}
