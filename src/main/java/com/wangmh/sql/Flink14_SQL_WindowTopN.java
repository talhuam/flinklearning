package com.wangmh.sql;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.temporal.Temporal;

public class Flink14_SQL_WindowTopN {

    public static void main(String[] args) {
        Configuration conf = new Configuration();
        // flink webUI port
        conf.setInteger("rest.port", 10000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        // 全局并行度
        env.setParallelism(2);

        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        tEnv.executeSql("create table user_behavior(" +
                " user_id bigint, " +
                " item_id bigint, " +
                " category_id int, " +
                " behavior string, " +
                " ts bigint, " +
                " event_time as to_timestamp_ltz(ts,3), " +
                " watermark for event_time as event_time - interval '5' second" +
                ") with (" +
                " 'connector'='filesystem'," +
                " 'path'='input/UserBehavior.csv'," +
                " 'format'='csv' " +
                ")");

        // 计算每个窗口每个商品的点击量
        Table t1 = tEnv.sqlQuery("select item_id, window_start, window_end, count(*) item_count" +
                "" +
                " from table(hop(table user_behavior, descriptor(event_time), interval '10' minute, interval '1' hour)) " +
                " where behavior='pv' group by item_id, window_start, window_end");

        tEnv.createTemporaryView("t1", t1);

        // over
        Table t2 = tEnv.sqlQuery("select item_id, window_start, window_end, item_count," +
                "row_number() over(partition by window_end order by item_count desc) rn " +
                "from t1");

        tEnv.createTemporaryView("t2", t2);

        // 过滤，取top3
        Table t3 = tEnv.sqlQuery("select item_id, window_start, window_end, item_count,rn from t2 where rn<=3");

        // 写入mysql
        tEnv.executeSql("create table hot_item(" +
                "w_end timestamp(3)," +
                "item_id bigint," +
                "item_count bigint," +
                "rk bigint," +
                "primary key (w_end,rk) not enforced" +
                ") with (" +
                " 'connector' = 'jdbc'," +
                " 'url' = 'jdbc:mysql://bigdata01:3306/flink_sql'," +
                " 'table-name' = 'hot_item'," +
                " 'username'='root'," +
                " 'password'='123456' " +
                ")");
        tEnv.executeSql("insert into hot_item select window_end, item_id, item_count, rn from " + t3);
    }

}
