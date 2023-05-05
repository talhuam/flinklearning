package com.wangmh.base;


import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * flink通过流处理方式处理无界流（socket端口模拟）
 */
public class Flink03_Streaming_Unbounded_WordCount {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // nc -lk 6666
        DataStreamSource<String> ds = env.socketTextStream("192.168.1.201", 6666);

        ds.flatMap((FlatMapFunction<String, Tuple2<String, Long>>) (s, collector) -> {
            String[] words = s.split(" ");
            for (String word : words) {
                collector.collect(Tuple2.of(word, 1l));
            }
        }).returns(Types.TUPLE(Types.STRING, Types.LONG))
                .keyBy((KeySelector<Tuple2<String, Long>, String>) tuple2 -> tuple2.f0)
                .sum(1)
                .print();

        env.execute();
    }

}
