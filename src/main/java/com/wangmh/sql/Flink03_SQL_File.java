package com.wangmh.sql;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class Flink03_SQL_File {

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
                "'connector' = 'filesystem', " +
                "'path' = 'input/sensor.json'," +
                "'format' = 'json'" +
                ")");
        Table result = tEnv.sqlQuery("select id,sum(vc) as vc from sensor group by id");

        tEnv.executeSql("create table sensor_tmp(" +
                "id string, " +
                "vc int" +
                ") with (" +
                "'connector' = 'filesystem', " +
                "'path' = 'input/sensor_tmp.txt'," +
                "'format' = 'csv'" +
                ")");
//        result.executeInsert("sensor_tmp");
        tEnv.executeSql("insert into sensor_tmp select * from " + result);
    }

}
