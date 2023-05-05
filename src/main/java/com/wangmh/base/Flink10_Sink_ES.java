package com.wangmh.base;

import com.alibaba.fastjson.JSON;
import com.wangmh.bean.WaterSensor;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch7.ElasticsearchSink;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.xcontent.XContentType;

import java.util.Arrays;
import java.util.List;

public class Flink10_Sink_ES {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        // flink webUI port
        conf.setInteger("rest.port", 10000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        // 全局并行度
        env.setParallelism(1);

        DataStreamSource<WaterSensor> stream = env.fromCollection(Arrays.asList(
                new WaterSensor("ws_001", 1577844001000l, 20),
                new WaterSensor("ws_002", 1577844001000l, 20),
                new WaterSensor("ws_001", 1577844001000l, 40),
                new WaterSensor("ws_001", 1577844001000l, 15),
                new WaterSensor("ws_002", 1577844001000l, 10)
        ));

        SingleOutputStreamOperator<WaterSensor> result = stream.keyBy(WaterSensor::getId).sum("vc");

        // es集群连接
        List<HttpHost> HttpHosts = Arrays.asList(
                new HttpHost("bigdata01", 9200),
                new HttpHost("bigdata02", 9200),
                new HttpHost("bigdata03", 9200)
        );
        ElasticsearchSink.Builder<WaterSensor> builder = new ElasticsearchSink.Builder<>(HttpHosts, new ElasticsearchSinkFunction<WaterSensor>() {
            @Override
            public void process(WaterSensor waterSensor, RuntimeContext runtimeContext, RequestIndexer requestIndexer) {
                String msg = JSON.toJSONString(waterSensor);

                IndexRequest indexRequest = Requests.indexRequest("water_sensor") // 索引名
                        .id(waterSensor.getId())
                        .source(msg, XContentType.JSON);

                requestIndexer.add(indexRequest); // 将数据添加，然后自动发送到es中
            }
        });

        builder.setBulkFlushInterval(2000); // 多久发一次
        builder.setBulkFlushMaxSizeMb(1); // 数据达到多大发一次，单位是M
        builder.setBulkFlushMaxActions(10); // 达到多少条发一次

        result.addSink(builder.build());

        env.execute("redisSink");
    }

}
