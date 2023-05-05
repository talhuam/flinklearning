package com.wangmh.base;

import com.wangmh.bean.WaterSensor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.util.Arrays;

public class Flink06_Process_NoKeyBy {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        // flink webUI port
        conf.setInteger("rest.port", 10000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        // 全局并行度
        env.setParallelism(1);

        env.fromCollection(Arrays.asList(
                new WaterSensor("ws_001", 1577844001000l, 20),
                new WaterSensor("ws_002", 1577844001000l, 20),
                new WaterSensor("ws_001", 1577844001000l, 40),
                new WaterSensor("ws_001", 1577844001000l, 15),
                new WaterSensor("ws_002", 1577844001000l, 10)
        )).process(new ProcessFunction<WaterSensor, String>() {
            // 如果线程数是2，则有两个sum
            int sum = 0;
            @Override
            public void processElement(WaterSensor waterSensor, // 输入的数据
                                       ProcessFunction<WaterSensor, String>.Context context, // 上下文
                                       Collector<String> collector) throws Exception {
                // 实现对vc的累加
                sum += waterSensor.getVc();
                collector.collect("总水位：" + sum);
            }
        }).print();

        env.execute("process");
    }

}
