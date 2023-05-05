package com.wangmh.base;

import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

public class Flink16_Broadcast_State {

    public static void main(String[] args) throws Exception {
//        Configuration conf = new Configuration();
//        // flink webUI port
//        conf.setInteger("rest.port", 10000);
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 全局并行度
        env.setParallelism(2);

        // 数据流
        DataStreamSource<String> dataStream = env.socketTextStream("bigdata01", 9999);

        // 配置流
        DataStreamSource<String> confStream = env.socketTextStream("bigdata01", 8888);

        // 1.把配置流做成广播流
        MapStateDescriptor<String, String> desc = new MapStateDescriptor<>("confState", String.class, String.class);
        BroadcastStream<String> bcStream = confStream.broadcast(desc);

        // 2.数据流connect广播流
        BroadcastConnectedStream<String, String> coStream = dataStream.connect(bcStream);

        coStream.process(new BroadcastProcessFunction<String, String, String>() {

            @Override
            public void processElement(String value, BroadcastProcessFunction<String, String, String>.ReadOnlyContext readOnlyContext, Collector<String> collector) throws Exception {
                // 4.在数据流中拿到广播状态
                ReadOnlyBroadcastState<String, String> broadcastState = readOnlyContext.getBroadcastState(desc);
                String state = broadcastState.get("switch");

                if (state == null){
                    System.out.println("执行default逻辑");
                } else if (state.equals("1")) {
                    System.out.println("执行1逻辑");
                } else if (state.equals("2")){
                    System.out.println("执行2逻辑");
                }else{
                    System.out.println("执行default逻辑");
                }
                collector.collect(value);
            }

            @Override
            public void processBroadcastElement(String value, BroadcastProcessFunction<String, String, String>.Context context, Collector<String> collector) throws Exception {
                // 3.在广播流中将数据放入广播状态
                BroadcastState<String, String> broadcastState = context.getBroadcastState(desc);
                broadcastState.put("switch", value);
            }
        }).print();

        env.execute("broadcast state");
    }

}
