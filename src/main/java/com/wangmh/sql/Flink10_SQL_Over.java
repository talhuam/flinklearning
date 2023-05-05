package com.wangmh.sql;

import com.wangmh.bean.WaterSensor;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Expressions;
import org.apache.flink.table.api.Over;
import org.apache.flink.table.api.OverWindow;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

import static org.apache.flink.table.api.Expressions.$;

public class Flink10_SQL_Over {

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
                new WaterSensor("sensor_1", 2000L, 30),
                new WaterSensor("sensor_1", 4000L, 40),
                new WaterSensor("sensor_1", 5000L, 50),
                new WaterSensor("sensor_1", 6001L, 60)
        ).assignTimestampsAndWatermarks( // 声明最大乱序程度和事件时间字段
                WatermarkStrategy.<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                        .withTimestampAssigner((ws, ts) -> ws.getTs())
        );

        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        // 把流转成Table，同时声明哪个字段是事件时间
        Table table = tEnv.fromDataStream(stream, $("id"), $("ts").rowtime(), $("vc"));
        tEnv.createTemporaryView("sensor", table);

        // rows
        tEnv.sqlQuery("select id,ts,vc," +
                "sum(vc) over(partition by id order by ts rows between unbounded preceding and current row) " +
                "from sensor")
                .execute().print();

        System.out.println("**********************************************");

        // range
        tEnv.sqlQuery("select id,ts,vc," +
                        "sum(vc) over(partition by id order by ts range between unbounded preceding and current row) " +
                        "from sensor")
                .execute().print();

        System.out.println("**********************************************");
        // 往前1秒
        tEnv.sqlQuery("select id,ts,vc," +
                        "sum(vc) over(partition by id order by ts rows between 1 preceding and current row) " +
                        "from sensor")
                .execute().print();
    }

}
