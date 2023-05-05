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
import org.apache.flink.table.functions.TableFunction;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;

public class Flink12_Table_Function {

    public static void main(String[] args) {
        Configuration conf = new Configuration();
        // flink webUI port
        conf.setInteger("rest.port", 2000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        // 全局并行度
        env.setParallelism(1);

        DataStreamSource<WaterSensor> dataStream = env.fromElements(
                new WaterSensor("flink spark", 1000L, 10),
                new WaterSensor("hdfs yarn", 2000L, 20),
                new WaterSensor("hive tez", 2000L, 30),
                new WaterSensor("elasticsearch logstash kibana", 4000L, 40)
        );

        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        Table table = tEnv.fromDataStream(dataStream);

        // 注册表
        tEnv.createTemporaryView("sensor", table);
        // 注册函数
        tEnv.createTemporaryFunction("explore", ExploreFunction.class);

        // Table API
        table
            .joinLateral(call("explore", $("id")))
            .select($("id"),$("word"),$("len"))
            .execute()
            .print();

        System.out.println("**********************************");

        // SQL
        // 内联：select * from a join b on a.id=b.id
        tEnv.sqlQuery("select id, word, len" +
                " from sensor join lateral table(explore(id)) on true")
                .execute()
                .print();

        // 去除join
        // 内联：select * from a,b where a.id=b.id
        tEnv.sqlQuery("select id, word, len" +
                        " from sensor,lateral table(explore(id))")
                .execute()
                .print();

        // 炸裂出来的字段取别名
        tEnv.sqlQuery("select id, w, l" +
                        " from sensor,lateral table(explore(id)) as t(w,l)")
                .execute()
                .print();
    }

    // 第一种实现方法
    // 定义输出的字段名
//    @FunctionHint(output = @DataTypeHint("row(word string,len int)"))
//    public static class ExploreFunction extends TableFunction<Row> {
//
//        public void eval(String value) {
//            String[] fields = value.split(" ");
//            for (String field : fields) {
//                collect(Row.of(field, field.length()));
//            }
//        }
//    }

    // 第二种实现方法，定义POJO
    public static class ExploreFunction extends TableFunction<WordLen> {

        public void eval(String value) {
            String[] fields = value.split(" ");
            for (String field : fields) {
                collect(new WordLen(field, field.length()));
            }
        }
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class WordLen{
        private String word;
        private Integer len;
    }

}
