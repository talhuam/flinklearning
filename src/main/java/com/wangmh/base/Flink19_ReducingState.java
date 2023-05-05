package com.wangmh.base;

import com.wangmh.bean.WaterSensor;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public class Flink19_ReducingState {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        // flink webUI port
        conf.setInteger("rest.port", 10000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        // 全局并行度
        env.setParallelism(2);

        DataStreamSource<String> stream = env.socketTextStream("bigdata01", 9999);
        stream.map(line -> {
                    String[] fields = line.split(",");
                    return new WaterSensor(fields[0], Long.parseLong(fields[1]), Integer.parseInt(fields[2]));
                })
                .keyBy(WaterSensor::getId)
                .process(new KeyedProcessFunction<String, WaterSensor, String>() {
                    private ReducingState<Integer> sumSate;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        // 获取状态
                        sumSate = getRuntimeContext().getReducingState(new ReducingStateDescriptor<Integer>("sumSate", new ReduceFunction<Integer>() {
                            @Override
                            public Integer reduce(Integer historyResult, Integer vc) throws Exception {
                                return historyResult + vc;
                            }
                        }, Integer.class));
                    }

                    @Override
                    public void processElement(WaterSensor waterSensor, KeyedProcessFunction<String, WaterSensor, String>.Context context, Collector<String> collector) throws Exception {
                        sumSate.add(waterSensor.getVc());
                        collector.collect(context.getCurrentKey() + "的水位和为" +sumSate.get());
                    }
                }).print();

        env.execute("reducingState");
    }
}
