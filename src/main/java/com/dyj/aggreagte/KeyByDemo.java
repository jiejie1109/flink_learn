package com.dyj.aggreagte;

import com.dyj.bean.watersenor;
import com.dyj.flinkfunction.MyKeyByFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class KeyByDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<watersenor> watersenorDataStreamSource = env.fromElements(
                new watersenor("s1", 1L, 1),
                new watersenor("s1", 1L, 1),
                new watersenor("s1", 1L, 1),
                new watersenor("s2", 2L, 2),
                new watersenor("s3", 3L, 3)
        );

        // 按照key 对数据进行分组，保证相同的key的数据在同一个分区
        KeyedStream<watersenor, String> keyedStream = watersenorDataStreamSource.keyBy(new MyKeyByFunction());

        keyedStream.print();
        env.execute();
    }
}
