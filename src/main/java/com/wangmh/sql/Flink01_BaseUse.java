package com.wangmh.sql;

import com.wangmh.bean.WaterSensor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;

public class Flink01_BaseUse {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        // flink webUI port
        conf.setInteger("rest.port", 10000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        // 全局并行度
        env.setParallelism(1);

        SingleOutputStreamOperator<WaterSensor> stream = env
                .readTextFile("input/sensor.txt")
                .map(s -> {
                    String[] fields = s.split(",");
                    return new WaterSensor(fields[0], Long.parseLong(fields[1]), Integer.parseInt(fields[2]));
                });

        // 1.创建执行环境
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        // 2.流转化为动态表
        Table table = tEnv.fromDataStream(stream);

        // 3.对动态表执行查询
//        Table result = table.select($("id"), $("vc"));
        Table result = table.groupBy($("id"))
                .select($("id"), $("vc").sum().as("vc_sum"));

        // 4.动态表转化成流
        DataStream<Tuple2<Boolean, Row>> resultStream = tEnv.toRetractStream(result, Row.class);
        resultStream.print();

        env.execute("flink sql base use");
    }

}
