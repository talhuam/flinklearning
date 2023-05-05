package com.wangmh.base;

import com.alibaba.fastjson.JSON;
import com.wangmh.bean.WaterSensor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;

import java.util.Arrays;

public class Flink09_Sink_Redis {

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

        SingleOutputStreamOperator<WaterSensor> result = stream.keyBy(WaterSensor::getId).sum("vc");

        FlinkJedisPoolConfig redisConfig = new FlinkJedisPoolConfig.Builder()
                .setHost("bigdata03")
                .setPort(6379)
                .setMaxTotal(100)
                .setMaxIdle(10)
                .setMinIdle(2)
                .setTimeout(10 * 1000)
                .build();

        result.addSink(new RedisSink<>(redisConfig, new RedisMapper<WaterSensor>() {
            // 获取命令描述符，就是对应redis的相关命令
            @Override
            public RedisCommandDescription getCommandDescription() {
                // 1.key value模式
//                return new RedisCommandDescription(RedisCommand.SET);
                // 2.list模式
//                return new RedisCommandDescription(RedisCommand.RPUSH);
                // 3.set模式，可去重
//                return new RedisCommandDescription(RedisCommand.SADD);
                // 4.hash模式，第二个参数是最外层的key
                // key field value
                return new RedisCommandDescription(RedisCommand.HSET, "s");
            }

            // 获取key
            @Override
            public String getKeyFromData(WaterSensor waterSensor) {
                return waterSensor.getId();
            }

            // 获取value
            @Override
            public String getValueFromData(WaterSensor waterSensor) {
                return JSON.toJSONString(waterSensor);
            }
        }));

        env.execute("redisSink");
    }

}
