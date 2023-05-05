package com.wangmh.sql;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class Flink05_SQL_Kafka_Upsert {

    public static void main(String[] args) {
        Configuration conf = new Configuration();
        // flink webUI port
        conf.setInteger("rest.port", 10000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        // 全局并行度
        env.setParallelism(2);

        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        tEnv.executeSql("create table sensor(" +
                "id string, " +
                "ts bigint," +
                "vc int" +
                ") with (" +
                "'connector' = 'kafka', " +
                "  'topic' = 'user_behavior'," +
                "  'properties.bootstrap.servers' = 'localhost:9092'," +
                "  'properties.group.id' = 'flink'," +
                "  'scan.startup.mode' = 'latest-offset'," +
                "  'format' = 'csv'" +
                ")");

        Table result = tEnv.sqlQuery("select id,sum(vc) as vc from sensor group by id");

        tEnv.executeSql("create table sensor_tmp(" +
                "id string, " +
                "vc int," +
                "primary key(id) not enforced" + // 主键是作为kafka的key
                ") with (" +
                "'connector' = 'upsert-kafka', " +
                "'properties.bootstrap.servers' = 'localhost:9092'," +
                "'topic' = 'topic_name'," +
                "'key.format' = 'json'," +
                "'value.format' = 'json'" +
                ")");
        result.executeInsert("sensor_tmp");
    }

}
