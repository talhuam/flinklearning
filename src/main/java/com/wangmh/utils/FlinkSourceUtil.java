package com.wangmh.utils;

import com.wangmh.common.FlinkConstance;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.Properties;

public class FlinkSourceUtil {

    public static FlinkKafkaConsumer<String> getKafkaSource(String groupId, String topic){
        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, FlinkConstance.BOOTSTRAP_SERVERS);
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        // 隔离级别，只读事务提交的数据
        props.setProperty(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        return new FlinkKafkaConsumer<String>(topic, new KafkaDeserializationSchema<String>() {
            // 是否终止流
            @Override
            public boolean isEndOfStream(String s) {
                return false;
            }

            // 反序列化
            @Override
            public String deserialize(ConsumerRecord<byte[], byte[]> consumerRecord) throws Exception {
                if (consumerRecord != null && consumerRecord.value() != null){
                    return new String(consumerRecord.value());
                }
                return null;
            }

            // 反序列化类型
            @Override
            public TypeInformation<String> getProducedType() {
                return TypeInformation.of(String.class);
            }
        }, props);
    }

}
