package com.dyj.wc;


import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class WordCountDemo_Stream {
    public static void main(String[] args) throws Exception {
        // TODO 创建流的执行环境
        StreamExecutionEnvironment en = StreamExecutionEnvironment.getExecutionEnvironment();
        // 读取数据
        DataStreamSource<String> streamSource = en.readTextFile("input/word.txt");

        // 切分 转换 分组 聚合
        SingleOutputStreamOperator<Tuple2<String, Integer>> outputStreamOperator = streamSource.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
                String[] wcs = s.split(" ");
                for (String wc : wcs) {
                    Tuple2<String, Integer> wc_tuple = Tuple2.of(wc, 1);
                    collector.collect(wc_tuple);
                }
            }
        });

        KeyedStream<Tuple2<String, Integer>, String> keyedStream = outputStreamOperator.keyBy(
                new KeySelector<Tuple2<String, Integer>, String>() {
                    @Override
                    public String getKey(Tuple2<String, Integer> value) throws Exception {
                        // 取第一个参数
                        return value.f0;
                    }
                }
        );
        SingleOutputStreamOperator<Tuple2<String, Integer>> sumDS = keyedStream.sum(1);
        sumDS.print();

        // 启动
        en.execute();
    }
}
