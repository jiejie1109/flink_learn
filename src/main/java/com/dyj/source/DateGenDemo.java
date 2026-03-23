package com.dyj.source;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class DateGenDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        /*
          数据生成器四个参数
          GeneratorFunction接口，需要实现，重写map方法 输入类型是LONG
          LONG类型，自动生抽数字序列的最大值
          限速策列
          返回类型
         */
        DataGeneratorSource<String> stringDataGeneratorSource = new DataGeneratorSource<>(
                new GeneratorFunction<Long, String>() {
                    @Override
                    public String map(Long aLong) throws Exception {
                        return "Number:" + aLong;
                    }
                },
                10,
                RateLimiterStrategy.perSecond(1),
                Types.STRING
        );

        env.fromSource(stringDataGeneratorSource, WatermarkStrategy.noWatermarks(),"DateGenDemo").print();

        env.execute();
    }
}
