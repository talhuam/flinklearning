package com.wangmh.base;

import com.wangmh.bean.WaterSensor;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.AggregatingState;
import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class Flink20_AggregatingState {

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
                    private AggregatingState<Integer, Double> avgState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        // 获取状态
                        avgState = getRuntimeContext().getAggregatingState(new AggregatingStateDescriptor<Integer, Tuple2<Integer, Integer>, Double>("avgState", new AggregateFunction<Integer, Tuple2<Integer, Integer>, Double>() {
                                @Override
                                public Tuple2<Integer, Integer> createAccumulator() {
                                    return Tuple2.of(0, 0);
                                }

                                @Override
                                public Tuple2<Integer, Integer> add(Integer value, Tuple2<Integer, Integer> acc) {
                                    acc.f0 += value;
                                    acc.f1++;
                                    return acc;
                                }

                                @Override
                                public Double getResult(Tuple2<Integer, Integer> acc) {
                                    return acc.f0 * 1.0 / acc.f1;
                                }
                                // 可以不用实现，只在session窗口有效
                                @Override
                                public Tuple2<Integer, Integer> merge(Tuple2<Integer, Integer> integerIntegerTuple2, Tuple2<Integer, Integer> acc1) {
                                    return null;
                                }
                            }, TypeInformation.of(new TypeHint<Tuple2<Integer, Integer>>(){}))
                        );
                    }

                    @Override
                    public void processElement(WaterSensor waterSensor, KeyedProcessFunction<String, WaterSensor, String>.Context context, Collector<String> collector) throws Exception {
                        avgState.add(waterSensor.getVc());
                        collector.collect(context.getCurrentKey() + "的平均水位为" + avgState.get());
                    }
                }).print();

        env.execute("AggregatingState");
    }
}
