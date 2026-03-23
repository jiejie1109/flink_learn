package com.dyj.flinkfunction;

import com.dyj.bean.watersenor;
import org.apache.flink.api.common.functions.MapFunction;

public  class mymapfunction implements MapFunction<watersenor,String> {
    @Override
    public String map(watersenor value) throws Exception {
        return value.getId();
    }
}
