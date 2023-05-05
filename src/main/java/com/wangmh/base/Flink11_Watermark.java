package com.wangmh.base;

import com.wangmh.bean.WaterSensor;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkGeneratorSupplier;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

public class Flink11_Watermark {

    public static void main(String[] args) throws Exception {
//        Configuration conf = new Configuration();
//        // flink webUI port
//        conf.setInteger("rest.port", 10000);
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 全局并行度
        env.setParallelism(1);

        DataStreamSource<String> stream = env.socketTextStream("bigdata01", 6666);

        stream.map(s -> {
                String[] fields = s.split(",");
                return new WaterSensor(fields[0], Long.parseLong(fields[1]), Integer.parseInt(fields[2]));
            })
            .assignTimestampsAndWatermarks(
                    // 3s钟的最大乱序
                    WatermarkStrategy.<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                            .withTimestampAssigner(new SerializableTimestampAssigner<WaterSensor>() {
                                @Override
                                public long extractTimestamp(WaterSensor waterSensor, long l) {
                                    // 声明事件时间字段
                                    return waterSensor.getTs();
                                }
                            })
                            .withIdleness(Duration.ofSeconds(5)) // 解决数据倾斜问题，如果一个并行度超过5秒不更新水印，则以其他并行度为准

            )
            .keyBy(WaterSensor::getId)
            .window(TumblingEventTimeWindows.of(Time.seconds(5)))
            .process(new ProcessWindowFunction<WaterSensor, String, String, TimeWindow>() {
                @Override
                public void process(String s, ProcessWindowFunction<WaterSensor, String, String, TimeWindow>.Context context, Iterable<WaterSensor> values, Collector<String> collector) throws Exception {
                    List<WaterSensor> waterSensors = new ArrayList<>();
                    for (WaterSensor value : values) {
                        waterSensors.add(value);
                    }
                    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                    String start = sdf.format(context.window().getStart());
                    String end = sdf.format(context.window().getEnd());
                    collector.collect(s + " " + start + "至" + end + waterSensors);
                }
            }).print();

        env.execute("watermark");
    }

}
