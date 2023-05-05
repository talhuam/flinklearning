package com.wangmh.sql;

import com.wangmh.bean.WaterSensor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.ScalarFunction;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;

public class Flink11_Scalar_Function {

    public static void main(String[] args) {
        Configuration conf = new Configuration();
        // flink webUI port
        conf.setInteger("rest.port", 2000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        // 全局并行度
        env.setParallelism(1);

        DataStreamSource<WaterSensor> dataStream = env.fromElements(
                new WaterSensor("sensor_1", 1000L, 10),
                new WaterSensor("sensor_1", 2000L, 20),
                new WaterSensor("sensor_1", 2000L, 30),
                new WaterSensor("sensor_1", 4000L, 40),
                new WaterSensor("sensor_1", 5000L, 50),
                new WaterSensor("sensor_1", 6001L, 60)
        );

        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        Table table = tEnv.fromDataStream(dataStream);

        // 注册表
        tEnv.createTemporaryView("sensor", table);
        // 注册函数
        tEnv.createTemporaryFunction("upper", UpperFunction.class);

        // Table API
        table
            .select($("id"), call("upper", $("id")))
            .execute()
            .print();

        System.out.println("**********************************");

        // SQL
        tEnv.sqlQuery("select id, upper(id)" +
                "from sensor").execute().print();
    }

    public static class UpperFunction extends ScalarFunction{

        public String eval(String value) {
            return value.toUpperCase();
        }

    }

}
