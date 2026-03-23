package com.dyj.flinkfunction;

import com.dyj.bean.watersenor;
import org.apache.flink.api.common.functions.FilterFunction;

public class MyFilterFunction implements FilterFunction<watersenor> {
    @Override
    public boolean filter(watersenor value) throws Exception {
        return "s1".equals(value.getId());
    }
}
