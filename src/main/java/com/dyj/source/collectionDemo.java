package com.dyj.source;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;

public class collectionDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //从集合读取数据
        DataStreamSource<Integer> sour = env.fromCollection(Arrays.asList(1, 2, 3));

        sour.print();

        env.execute();
    }
}
