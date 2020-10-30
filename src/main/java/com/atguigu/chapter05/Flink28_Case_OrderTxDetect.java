package com.atguigu.chapter05;

import com.atguigu.bean.AdClickLog;
import com.atguigu.bean.OrderEvent;
import com.atguigu.bean.TxEvent;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.Map;

/**
 * 实时对账
 *
 * @author cjp
 * @version 1.0
 * @date 2020/10/28 16:39
 */
public class Flink28_Case_OrderTxDetect {
    public static void main(String[] args) throws Exception {
        // 0.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        // 1. 读取数据
        // 1.1 读取 业务系统的 数据
        SingleOutputStreamOperator<OrderEvent> orderDS = env
                .readTextFile("input/OrderLog.csv")
                .map(
                        new MapFunction<String, OrderEvent>() {
                            @Override
                            public OrderEvent map(String value) throws Exception {
                                String[] datas = value.split(",");
                                return new OrderEvent(
                                        Long.valueOf(datas[0]),
                                        datas[1],
                                        datas[2],
                                        Long.valueOf(datas[3])
                                );
                            }
                        }
                );
        // 1.2 读取 交易系统的 数据
        SingleOutputStreamOperator<TxEvent> txDS = env
                .readTextFile("input/ReceiptLog.csv")
                .map(
                        new MapFunction<String, TxEvent>() {
                            @Override
                            public TxEvent map(String value) throws Exception {
                                String[] datas = value.split(",");
                                return new TxEvent(
                                        datas[0],
                                        datas[1],
                                        Long.valueOf(datas[2])
                                );
                            }
                        }
                );

        // 2. 处理数据
        // 2.1 关联两条流，使用 connect
        ConnectedStreams<OrderEvent, TxEvent> orderTxCS = orderDS.connect(txDS);


        // 2.2 使用 ProcessFunction
        SingleOutputStreamOperator<String> resultDS = orderTxCS
                .keyBy(order -> order.getTxId(), tx -> tx.getTxId())    //连接两条流，如果使用连接条件进行匹配，要对 连接条件 进行keyby，避免多并行度的影响
                .process(new OrderTxDetectFunction());

        // 3. 输出
        resultDS.print();

        env.execute();
    }

    public static class OrderTxDetectFunction extends CoProcessFunction<OrderEvent, TxEvent, String> {

        private Map<String, TxEvent> txEventMap = new HashMap<>();
        private Map<String, OrderEvent> orderMap = new HashMap<>();

        /**
         * 处理 业务系统的 数据：一条一条处理
         *
         * @param value
         * @param ctx
         * @param out
         * @throws Exception
         */
        @Override
        public void processElement1(OrderEvent value, Context ctx, Collector<String> out) throws Exception {
            // 进入这个方法，说明处理的是 业务系统的 数据
            // 判断，同一个交易码的交易系统的数据 来过没有？
            TxEvent txEvent = txEventMap.get(value.getTxId());
            if (txEvent == null) {
                // 1.同一个交易码的 交易系统的数据，没来过 => 把 自己（业务系统的数据） 保存起来，等待 交易系统的数据
                orderMap.put(value.getTxId(), value);
            } else {
                // 2.同一个交易码的 交易系统的数据，来过 => 匹配上
                out.collect("订单" + value.getOrderId() + "通过交易码=" + value.getTxId() + "对账成功！！！");
                txEventMap.remove(value.getTxId());
            }
        }

        /**
         * 处理 交易系统的 数据：一条一条处理
         *
         * @param value
         * @param ctx
         * @param out
         * @throws Exception
         */
        @Override
        public void processElement2(TxEvent value, Context ctx, Collector<String> out) throws Exception {
            // 进入这个方法，说明处理的是 交易系统的 数据
            // 判断，同一个交易码的业务系统的数据 来过没有？
            OrderEvent orderEvent = orderMap.get(value.getTxId());
            if (orderEvent == null) {
                // 1.同一个交易码的 业务系统的数据，没来过 => 把 自己（交易系统的数据） 保存起来，等待 业务系统的数据
                txEventMap.put(value.getTxId(), value);
            } else {
                // 2.同一个交易码的 业务系统的数据，来过 => 匹配上
                out.collect("订单" + orderEvent.getOrderId() + "通过交易码=" + value.getTxId() + "对账成功！！！");
                orderMap.remove(value.getTxId());
            }
        }
    }

}
