package com.dyj.flinkfunction;

import com.dyj.bean.watersenor;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

public class MyFlatMapFunction implements FlatMapFunction<watersenor,String> {
    @Override
    public void flatMap(watersenor value, Collector<String> out) throws Exception {
        if ("s1".equals(value.getId())){
            out.collect(value.getVc().toString());
        }else if("s2".equals(value.getId())){
            out.collect(value.getTs().toString());
            out.collect(value.getVc().toString());
        }
    }
}
