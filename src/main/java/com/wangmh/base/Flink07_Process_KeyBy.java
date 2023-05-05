package com.wangmh.base;

import com.wangmh.bean.WaterSensor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class Flink07_Process_KeyBy {

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
        )).keyBy(WaterSensor::getId).process(new KeyedProcessFunction<String, WaterSensor, String>() {
            // 第一个泛型是key的类型，第二个泛型是输入类型，第三个是输出类型
            Map<String, Integer> map = new HashMap<>();
            @Override
            public void processElement(WaterSensor waterSensor, KeyedProcessFunction<String, WaterSensor, String>.Context context, Collector<String> collector) throws Exception {
                int sum = map.getOrDefault(context.getCurrentKey(), 0);
                sum += waterSensor.getVc();
                map.put(context.getCurrentKey(), sum);

                collector.collect(waterSensor.getId()+"的累计水位是："+sum);
            }
        }).print();

        env.execute("process");
    }

}
