package com.dyj.source;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;

public class KafkaDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        KafkaSource<String> build = KafkaSource.<String>builder()
                .setBootstrapServers("hadoop102:9092,hadoop103:9092,hadoop104:9092")  //指定地址端口
                .setGroupId("daiyj") //指定消费者组
                .setTopics("topic_1") //指定消费topic
                .setValueOnlyDeserializer(new SimpleStringSchema())  //指定反序列化器
                .setStartingOffsets(OffsetsInitializer.latest())   //flink消费kafka的策略
                .build();


        // 从kafka读取
        env.fromSource(build, WatermarkStrategy.noWatermarks(),"kafka_source").print();


        env.execute();
    }
}
