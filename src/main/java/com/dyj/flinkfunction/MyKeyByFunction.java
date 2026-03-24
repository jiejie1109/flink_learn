package com.dyj.flinkfunction;

import com.dyj.bean.watersenor;
import org.apache.flink.api.java.functions.KeySelector;

public class MyKeyByFunction implements KeySelector<watersenor,String> {
    @Override
    public String getKey(watersenor value) throws Exception {
        return value.getId();
    }
}
