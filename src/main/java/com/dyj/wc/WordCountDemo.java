package com.dyj.wc;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;


public class WordCountDemo {
    public static void main(String[] args) throws Exception {
        // TODO 创建执行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        // TODO 读取数据
        DataSource<String> stringDataSource = env.readTextFile("input/word.txt");
        // TODO 切分转换
        FlatMapOperator<String, Tuple2<String, Integer>> word_tupl = stringDataSource.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
                // 按照空格切分
                String[] split_s = s.split(" ");
                // 进行转换
                for (String wc : split_s) {
                    Tuple2<String, Integer> word_tuple = Tuple2.of(wc, 1);
                    // 调用采集器
                    collector.collect(word_tuple);
                }
            }
        });
        // TODO 分组聚合  第一个参数为需要分组的位置
        UnsortedGrouping<Tuple2<String, Integer>> tuple2UnsortedGrouping = word_tupl.groupBy(0);
        // 第二个是需要 聚合的位置
        AggregateOperator<Tuple2<String, Integer>> sum = tuple2UnsortedGrouping.sum(1);
        // TODO 输出
        sum.print();
    }
}
