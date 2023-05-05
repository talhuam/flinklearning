package com.wangmh.base;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

public class Flink05_Connect {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        // flink webUI port
        conf.setInteger("rest.port", 10000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        // 全局并行度
        env.setParallelism(1);

        DataStreamSource<Integer> ds1 = env.fromElements(10, 20, 12, 11, 9);
        DataStreamSource<String> ds2 = env.fromElements("a", "c", "b");

        // 泛型1：第一个流的类型；泛型2：第二个流的类型；泛型3：输出类型
        ds1.connect(ds2).map(new CoMapFunction<Integer, String, String>() {
            @Override
            public String map1(Integer value) throws Exception {
                return value + "<";
            }

            @Override
            public String map2(String value) throws Exception {
                return value + ">";
            }
        }).print();

        env.execute("connect");
    }
}
