package com.wangmh.base;

import com.wangmh.bean.WaterSensor;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public class Flink18_ListState {

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

                    private ListState<Integer> listState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        // 获取状态
                        listState = getRuntimeContext().getListState(new ListStateDescriptor<Integer>("top3VcState", Integer.class));
                    }

                    @Override
                    public void processElement(WaterSensor waterSensor, KeyedProcessFunction<String, WaterSensor, String>.Context context, Collector<String> collector) throws Exception {
                        listState.add(waterSensor.getVc());

                        Iterable<Integer> valueIter = listState.get();
                        List<Integer> values = new ArrayList<>();
                        for (Integer value : valueIter) {
                            values.add(value);
                        }

                        // 排序
//                        1.第一种方式
//                        values.sort(new Comparator<Integer>() {
//                            @Override
//                            public int compare(Integer o1, Integer o2) {
//                                // 倒序
//                                return o2.compareTo(o1);
//                            }
//                        });
//                        2.第二种方式，lambda
//                        values.sort(((o1, o2) -> o2.compareTo(o1)));
//                        3.第三种方式
                        values.sort(Comparator.reverseOrder());

                        if (values.size() == 4){
                            values.remove(values.size() - 1);
                        }

                        collector.collect(values.toString());

                        // 更新状态
                        listState.update(values);
                    }
                }).print();

        env.execute("listState");
    }

}
