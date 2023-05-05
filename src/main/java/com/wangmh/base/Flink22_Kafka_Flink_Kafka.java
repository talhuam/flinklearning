package com.wangmh.base;


import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.nio.charset.StandardCharsets;
import java.util.Properties;

/**
 * kafka-flink-kafka端对端的一致性
 * exactly-once
 */
public class Flink22_Kafka_Flink_Kafka {

    public static void main(String[] args) throws Exception {
        System.setProperty("HADOOP_USER_NAME", "wmh");
        Configuration conf = new Configuration();
        // flink webUI port
        conf.setInteger("rest.port", 10000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        // 全局并行度
        env.setParallelism(2);

        // 开启checkpoint
        env.enableCheckpointing(2000);
        env.setStateBackend(new HashMapStateBackend());
        // 设置checkpoint目录
        env.getCheckpointConfig().setCheckpointStorage("hdfs://bigdata01:9000/user/wmh/ck1");
        // 并发的checkpoint个数，1表示只有一个barrier，只有checkpoint完成，才生成下一个barrier
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        // 设置barrier对齐，严格一次
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);


        Properties sourceProp = new Properties();
        sourceProp.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "bigdata01:9092,bigdata02:9092,bigdata03:9092");
        sourceProp.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "flink");
        // 只读事务已经提交的数据
        sourceProp.setProperty(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");

        SingleOutputStreamOperator<Tuple2<String, Long>> stream = env
                .addSource(new FlinkKafkaConsumer<String>("flink-topic-1", new SimpleStringSchema(), sourceProp))
                .flatMap(new FlatMapFunction<String, Tuple2<String, Long>>() {
                    @Override
                    public void flatMap(String s, Collector<Tuple2<String, Long>> collector) throws Exception {
                        String[] words = s.split(" ");
                        for (String word : words) {
                            collector.collect(Tuple2.of(word, 1l));
                        }
                    }
                })
                .keyBy(t -> t.f0)
                .sum(1);

        Properties sinkProp = new Properties();
        sinkProp.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "bigdata01:9092,bigdata02:9092,bigdata03:9092");
        // 事务超时时间，kafka服务超时是15分钟，flink默认是一个小时，所以需要设置
        sinkProp.put("transaction.timeout.ms", 15 * 60 * 1000);

        stream.addSink(new FlinkKafkaProducer<Tuple2<String, Long>>(
                "default",
                new KafkaSerializationSchema<Tuple2<String, Long>>() {
                    @Override
                    public ProducerRecord<byte[], byte[]> serialize(Tuple2<String, Long> value, @Nullable Long aLong) {
                        return new ProducerRecord<>("flink-topic-2", (value.f0 + "_" + value.f1).getBytes(StandardCharsets.UTF_8));
                    }
                },
                sinkProp,
                FlinkKafkaProducer.Semantic.EXACTLY_ONCE
        ));

        env.execute("exactly-once");
    }

}
