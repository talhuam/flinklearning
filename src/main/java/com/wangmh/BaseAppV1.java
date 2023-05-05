package com.wangmh;

import com.wangmh.utils.FlinkSourceUtil;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 任务的基类，继承BaseAppV1，实现抽象方法handle
 */
public abstract class BaseAppV1 {

    protected abstract void handle(DataStreamSource<String> stream);

    public void init(int port, int parallelism, String ckPathGroupIdJobName, String topic){
//        System.setProperty("HADOOP_USER_NAME","wmh");
        Configuration conf = new Configuration();
        // flink webUI port
        conf.setInteger("rest.port", port);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(parallelism);

        // 状态后端，生产环境建议使用RocksDB
        env.setStateBackend(new HashMapStateBackend());
        // checkpoint相关设置
        env.enableCheckpointing(3000);
        env.getCheckpointConfig().setCheckpointStorage("hdfs://bigdata01:8020/realtime/" + ckPathGroupIdJobName);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(30 *1000);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        // 重启策略
        env.setRestartStrategy(RestartStrategies.failureRateRestart(3, Time.days(1), Time.minutes(1)));

        DataStreamSource<String> stream = env.addSource(FlinkSourceUtil.getKafkaSource(ckPathGroupIdJobName, topic));

        handle(stream);

        try {
            env.execute(ckPathGroupIdJobName);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

}
