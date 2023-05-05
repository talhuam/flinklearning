package com.wangmh.base;

import com.wangmh.bean.WaterSensor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class Flink14_Timer_Pt {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        // flink webUI port
        conf.setInteger("rest.port", 10000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        // 全局并行度
        env.setParallelism(1);

        DataStreamSource<String> stream = env.socketTextStream("bigdata01", 6666);

        stream.map(s -> {
            String[] fields = s.split(",");
            return new WaterSensor(fields[0], Long.parseLong(fields[1]), Integer.parseInt(fields[2]));
        })
        .keyBy(WaterSensor::getId)
        .process(new KeyedProcessFunction<String, WaterSensor, String>() {
            long ts = 0;
            // 需求：水位大于10则5秒后触发定时器
            @Override
            public void processElement(WaterSensor waterSensor, KeyedProcessFunction<String, WaterSensor, String>.Context context, Collector<String> collector) throws Exception {
                if (waterSensor.getVc() > 10){
                    // 注册定时器
                    ts = System.currentTimeMillis() + 5000;
                    context.timerService().registerProcessingTimeTimer(ts);
                    System.out.println(context.getCurrentKey() + ":注册定时器" + ts);
                }else {
                    context.timerService().deleteProcessingTimeTimer(ts);
                    System.out.println(context.getCurrentKey() + ":取消定时器" + ts);
                }
            }

            // 定时器触发，则会执行onTimer的逻辑
            @Override
            public void onTimer(long timestamp, KeyedProcessFunction<String, WaterSensor, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
                out.collect(ctx.getCurrentKey() + "的水位上升了，触发报警");
            }
        }).print();

        env.execute("Timer_Ps");
    }

}