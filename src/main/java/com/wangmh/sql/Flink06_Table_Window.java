package com.wangmh.sql;

import com.wangmh.bean.WaterSensor;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.*;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.lit;

public class Flink06_Table_Window {

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

        // 滚动窗口(窗口大小5s)
//        TumbleWithSizeOnTimeWithAlias window = Tumble.over(lit(5).second()).on($("ts")).as("w");
        // 滑动窗口(窗口大小5s，滑动步长2s)
//        SlideWithSizeAndSlideOnTimeWithAlias window = Slide.over(lit(5).second()).every(lit(2).second()).on($("ts")).as("w");
        // 会哈窗口(间隙2s)
        SessionWithGapOnTimeWithAlias window = Session.withGap(lit(2).second()).on($("ts")).as("w");
        table.window(window)
                .groupBy($("id"), $("w"))
                .select($("id"), $("w").start(), $("w").end(), $("vc").sum().as("vc_sum"))
                .execute()
                .print();

    }

}
