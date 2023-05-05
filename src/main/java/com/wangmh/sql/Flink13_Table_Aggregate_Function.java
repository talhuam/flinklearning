package com.wangmh.sql;

import com.wangmh.bean.WaterSensor;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.functions.TableAggregateFunction;
import org.apache.flink.util.Collector;

import java.nio.file.FileStore;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;

public class Flink13_Table_Aggregate_Function {

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
        tEnv.createTemporaryFunction("topN", Top2Function.class);

        table
            .groupBy($("id"))
            .flatAggregate(call("topN", $("vc")))
            .select($("id"), $("name"), $("value"))
            .execute().print();
    }

    public static class Top2Function extends TableAggregateFunction<Result, FirstSecond> {

        @Override
        public FirstSecond createAccumulator() {
            return new FirstSecond();
        }

        // 做累加操作
        public void accumulate(FirstSecond fs, Integer vc){
            if (vc > fs.first){
                fs.second = fs.first;
                fs.first = vc;
            }else if (vc > fs.second) {
                fs.second = vc;
            }
        }

        // 发送数据
        // 第一个参数是累加器
        // 第二个参数是collector
        public void emitValue(FirstSecond fs, Collector<Result> collector){
            if (fs.first > Integer.MIN_VALUE) {
                collector.collect(new Result("第一名", fs.first));
            }
            if (fs.second > Integer.MIN_VALUE) {
                collector.collect(new Result("第二名", fs.second));
            }
        }
    }

    public static class FirstSecond{
        public Integer first=Integer.MIN_VALUE;
        public Integer second=Integer.MIN_VALUE;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Result{
        private String name;
        private Integer value;
    }

}
