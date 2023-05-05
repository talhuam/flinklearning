package com.wangmh.sql;

import com.wangmh.bean.WaterSensor;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.lit;

public class Flink07_SQL_Window {

    public static void main(String[] args) {
        Configuration conf = new Configuration();
        // flink webUI port
        conf.setInteger("rest.port", 10000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        // 全局并行度
        env.setParallelism(1);

        DataStream<WaterSensor> stream = env.fromElements(
                new WaterSensor("sensor_1", 1000L, 10),
                new WaterSensor("sensor_1", 2000L, 20),
                new WaterSensor("sensor_2", 3000L, 30),
                new WaterSensor("sensor_1", 4000L, 40),
                new WaterSensor("sensor_2", 5000L, 50),
                new WaterSensor("sensor_1", 6001L, 60)
        ).assignTimestampsAndWatermarks( // 声明最大乱序程度和事件时间字段
                WatermarkStrategy.<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                        .withTimestampAssigner((ws, ts) -> ws.getTs())
        );

        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        // 把流转成Table，同时声明哪个字段是事件时间
        Table table = tEnv.fromDataStream(stream, $("id"), $("ts").rowtime(), $("vc"));

        // 注册表
        tEnv.createTemporaryView("sensor", table);

        // 滚动窗口(5s)
        tEnv.sqlQuery("select " +
                "id, " +
                "tumble_start(ts, interval '5' second) as start_time, " +
                "tumble_end(ts, interval '5' second) as end_time, " +
                "sum(vc) vc_sum " +
                "from sensor " +
                "group by id, tumble(ts, interval '5' second)")
                .execute()
                .print();

        System.out.println("****************************************");

        // 滑动窗口(5s，滑动步长2s)
        tEnv.sqlQuery("select " +
                        "id, " +
                        "hop_start(ts, interval '2' second, interval '5' second) as start_time, " +
                        "hop_end(ts, interval '2' second, interval '5' second) as end_time, " +
                        "sum(vc) vc_sum " +
                        "from sensor " +
                        "group by id, hop(ts, interval '2' second, interval '5' second)")
                .execute()
                .print();

        System.out.println("****************************************");

        // 滑动窗口(5s，滑动步长2s)
        tEnv.sqlQuery("select " +
                        "id, " +
                        "session_start(ts, interval '2' second) as start_time, " +
                        "session_end(ts, interval '2' second) as end_time, " +
                        "sum(vc) vc_sum " +
                        "from sensor " +
                        "group by id, session(ts, interval '2' second)")
                .execute()
                .print();
    }

}
