package com.wangmh.base;

import com.wangmh.bean.WaterSensor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;


/**
 * 分流
 */
public class Flink13_Split_Stream {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        // flink webUI port
        conf.setInteger("rest.port", 10000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        // 全局并行度
        env.setParallelism(1);

        DataStreamSource<String> stream = env.socketTextStream("bigdata01", 6666);

        SingleOutputStreamOperator<String> main = stream.map(s -> {
                    String[] fields = s.split(",");
                    return new WaterSensor(fields[0], Long.parseLong(fields[1]), Integer.parseInt(fields[2]));
                })
                .process(new ProcessFunction<WaterSensor, String>() {
                    @Override
                    public void processElement(WaterSensor waterSensor, ProcessFunction<WaterSensor, String>.Context context, Collector<String> collector) throws Exception {
                        // 分成三个流
                        collector.collect(waterSensor.toString());
                        if (waterSensor.getId().equals("sensor01")) {
                            context.output(new OutputTag<String>("sensor01"){}, waterSensor.toString());
                        } else {
                            context.output(new OutputTag<String>("other"){}, waterSensor.toString());
                        }
                    }
                });

        main.print("main");
        // 获取侧输出
        main.getSideOutput(new OutputTag<String>("sensor01"){}).print("sensor01");
        main.getSideOutput(new OutputTag<String>("other"){}).print("other");

        env.execute("watermark");
    }

}
