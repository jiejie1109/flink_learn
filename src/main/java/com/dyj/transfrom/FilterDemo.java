package com.dyj.transfrom;

import com.dyj.bean.watersenor;
import com.dyj.flinkfunction.MyFilterFunction;
import com.dyj.flinkfunction.mymapfunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class FilterDemo {
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

        // true保留，false去掉
        SingleOutputStreamOperator<watersenor> filter_id = watersenorDataStreamSource.filter(new MyFilterFunction());

        filter_id.print();
        env.execute();

    }
}
