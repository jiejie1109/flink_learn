package com.dyj.transfrom;

import com.dyj.bean.watersenor;
import com.dyj.flinkfunction.mymapfunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class MapDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<watersenor> watersenorDataStreamSource = env.fromElements(
                new watersenor("s1", 1L, 1),
                new watersenor("s2", 2L, 2),
                new watersenor("s3", 3L, 3)
        );

        // 方式1 匿名实现类
//        SingleOutputStreamOperator<String> map_id = watersenorDataStreamSource.map(new MapFunction<watersenor, String>() {
//            @Override
//            public String map(watersenor value) throws Exception {
//                return value.getId();
//            }
//        });

        //方式2 定义一个类实现MapFunction
        SingleOutputStreamOperator<String> map_id = watersenorDataStreamSource.map(new mymapfunction());

        map_id.print();

        env.execute();

    }
}
