package com.wangmh.base;

import com.wangmh.bean.WaterSensor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;
import java.util.Arrays;

public class Flink04_Transform_Reduce {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 全局并行度
        env.setParallelism(1);

        env.fromCollection(Arrays.asList(
                new WaterSensor("ws_001", 1577844001000l, 20),
                new WaterSensor("ws_002", 1577844001000l, 20),
                new WaterSensor("ws_001", 1577844001000l, 40),
                new WaterSensor("ws_001", 1577844001000l, 15),
                new WaterSensor("ws_002", 1577844001000l, 10)
        )).keyBy(WaterSensor::getId)
                .reduce((ws1, ws2) -> {
                    // ws1是历史聚合的结果，ws2是新来的一条数据
            return new WaterSensor(ws1.getId(), ws2.getTs(), ws1.getVc() + ws2.getVc());
        }).print();



        env.execute("reduce");
    }
}
