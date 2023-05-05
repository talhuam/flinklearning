package com.wangmh.base;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.guava18.com.google.common.hash.BloomFilter;
import org.apache.flink.shaded.guava18.com.google.common.hash.Funnels;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class Flink23_BloomFilter {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        // flink webUI port
        conf.setInteger("rest.port", 10000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        // 全局并行度
        env.setParallelism(2);

        env.readTextFile("input/UserBehavior.csv").map(l -> {
            String[] fields = l.split(",");
            return new UserBehavior(
                    Long.parseLong(fields[0]),
                    Long.parseLong(fields[1]),
                    Integer.parseInt(fields[2]),
                    fields[3],
                    Long.parseLong(fields[4])
                    );
        })
        .filter(ub -> ub.getBehavior().equals("pv"))
        .assignTimestampsAndWatermarks(
                WatermarkStrategy.<UserBehavior>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withTimestampAssigner((ws, ts) -> ws.getTs() * 1000)
        )
        .keyBy(UserBehavior::getBehavior)
        .window(TumblingEventTimeWindows.of(Time.minutes(60)))
        .process(new ProcessWindowFunction<UserBehavior, String, String, TimeWindow>() {
            private ValueState<BloomFilter<Long>> bloomFilter;
            private ValueState<Long> countState;

            @Override
            public void open(Configuration parameters) throws Exception {
                System.out.println("Flink23_BloomFilter.open");
                countState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("countState", Long.class));

                bloomFilter = getRuntimeContext().getState(new ValueStateDescriptor<BloomFilter<Long>>("bloomFilter", TypeInformation.of(new TypeHint<BloomFilter<Long>>() {
                })));
            }

            @Override
            public void process(String s, ProcessWindowFunction<UserBehavior, String, String, TimeWindow>.Context context, Iterable<UserBehavior> iterable, Collector<String> collector) throws Exception {
                countState.update(0l);

                // 参数1：存储类型
                // 参数2：期望插入的元素个数
                // 参数3：期望的误判率(假阳性)
                BloomFilter<Long> bf = BloomFilter.create(Funnels.longFunnel(), 100 * 10000, 0.001);
                bloomFilter.update(bf);

                for (UserBehavior ub : iterable) {
                    if (!bloomFilter.value().mightContain(ub.getUserId())) {
                        // 不存在则计数
                        countState.update(countState.value() + 1);
                        // 记录这个用户
                        bloomFilter.value().put(ub.getUserId());
                    }
                }

                collector.collect("窗口: " + context.window() + " 的uv是: " + countState.value());
            }
        }).print();

        env.execute("bloom_filter");
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class UserBehavior{
        private Long userId;
        private Long itemId;
        private Integer category;
        private String behavior;
        private Long ts;
    }

}
