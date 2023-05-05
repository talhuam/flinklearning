package com.wangmh.sql;

import com.wangmh.bean.WaterSensor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.functions.ScalarFunction;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;

public class Flink13_Aggregate_Function {

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
                new WaterSensor("sensor_2", 2000L, 30),
                new WaterSensor("sensor_1", 4000L, 40),
                new WaterSensor("sensor_1", 5000L, 50),
                new WaterSensor("sensor_1", 6001L, 60)
        );

        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        Table table = tEnv.fromDataStream(dataStream);

        // 注册表
        tEnv.createTemporaryView("sensor", table);
        // 注册函数
        tEnv.createTemporaryFunction("avg_func", AvgFunction.class);

        // Table API
        table
            .groupBy($("id"))
            .aggregate(call("avg_func", $("vc")).as("vc_avg"))
            .select($("id"), $("vc_avg"))
            .execute().print();

        System.out.println("**********************************");

        // SQL
        tEnv.sqlQuery("select id, avg_func(vc) vc_avg " +
                "from sensor group by id").execute().print();
    }

    public static class AvgFunction extends AggregateFunction<Double, SumCount> {

        // 得到结果
        @Override
        public Double getValue(SumCount sumCount) {
            return sumCount.sum * 1.0 / sumCount.count;
        }

        public void accumulate(SumCount sc, Integer vc){
            sc.sum += vc;
            sc.count++;
        }

        // 创建累加器
        @Override
        public SumCount createAccumulator() {
            return new SumCount();
        }
    }

    public static class SumCount{
        public Integer sum=0;
        public Long count=0l;
    }

}
