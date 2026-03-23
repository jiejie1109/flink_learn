package com.dyj.enviorment;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class env_demo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.execute();

    }
}
