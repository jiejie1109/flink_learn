package com.dyj.aggreagte;

import com.dyj.bean.watersenor;
import com.dyj.flinkfunction.MyKeyByFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class ReduceDemo {
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

        KeyedStream<watersenor, String> keyedStream = watersenorDataStreamSource.keyBy(new MyKeyByFunction());

        //reduce  两两相加 输入类型等于输出类型
        //reduce 中的两个参数 value1 是之前的计算结果 value2是最新的
        SingleOutputStreamOperator<watersenor> reduced = keyedStream.reduce(new ReduceFunction<watersenor>() {
            @Override
            public watersenor reduce(watersenor value1, watersenor value2) throws Exception {
                System.out.println("value1"+value1);
//                System.out.println(value2);
                return new watersenor(value1.id, value2.ts, value1.vc + value2.vc);
            }
        });

        reduced.print();

        env.execute();
    }
}
