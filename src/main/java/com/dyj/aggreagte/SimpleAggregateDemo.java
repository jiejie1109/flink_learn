package com.dyj.aggreagte;

import com.dyj.bean.watersenor;
import com.dyj.flinkfunction.MyKeyByFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class SimpleAggregateDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<watersenor> watersenorDataStreamSource = env.fromElements(
                new watersenor("s1", 1L, 1),
                new watersenor("s1", 1L, 1),
                new watersenor("s1", 1L, 2),
                new watersenor("s2", 2L, 2),
                new watersenor("s3", 3L, 1),
                new watersenor("s3", 3L, 3)
        );

        // 按照key 对数据进行分组，保证相同的key的数据在同一个分区
        KeyedStream<watersenor, String> keyedStream = watersenorDataStreamSource.keyBy(new MyKeyByFunction());
        /**
         * 聚合算子  pojo类型需要传字段名称
         */
        SingleOutputStreamOperator<watersenor> sum_result = keyedStream.sum("vc");
        // um_result.print();

        /**
         * 最小值
         */
        SingleOutputStreamOperator<watersenor> minned = keyedStream.min("vc");

        minned.print();

        //max与maxBy的区别 max只会保留最大值的字段 maxby会保留最大值的数据

        // keyedStream.print();
        env.execute();
    }
}
