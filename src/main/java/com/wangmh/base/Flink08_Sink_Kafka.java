package com.wangmh.base;

import com.alibaba.fastjson.JSON;
import com.wangmh.bean.WaterSensor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Properties;

public class Flink08_Sink_Kafka {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        // flink webUI port
        conf.setInteger("rest.port", 10000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        // 全局并行度
        env.setParallelism(1);

        DataStreamSource<WaterSensor> stream = env.fromCollection(Arrays.asList(
                new WaterSensor("ws_001", 1577844001000l, 20),
                new WaterSensor("ws_002", 1577844001000l, 20),
                new WaterSensor("ws_001", 1577844001000l, 40),
                new WaterSensor("ws_001", 1577844001000l, 15),
                new WaterSensor("ws_002", 1577844001000l, 10)
        ));

        // kafka配置
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "bigdata01:9092");

        stream.keyBy(WaterSensor::getId).sum("vc")
                        .addSink(new FlinkKafkaProducer<WaterSensor>(
                                "default", // 默认的topic，一般用不上
                                new KafkaSerializationSchema<WaterSensor>() {
                                    // 通常自定义序列化器，因为这样才能指定时间语义
                                    @Override
                                    public ProducerRecord<byte[], byte[]> serialize(WaterSensor waterSensor, @Nullable Long timestamp) {
                                        String s = JSON.toJSONString(waterSensor);
                                        return new ProducerRecord<>("education-online", s.getBytes(StandardCharsets.UTF_8));
                                    }
                                },
                                properties, // kafka配置
                                FlinkKafkaProducer.Semantic.AT_LEAST_ONCE // 时间语义
                        ));

        env.execute("kafkasink");
    }
    
}
