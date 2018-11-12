package com.ht.kafka.paperreport.test;

import com.ht.utils.JRedisUtil;
import com.ht.utils.ConfigHelper;

import java.util.Properties;

public class Test {
    public static void main(String[] args) {
        JRedisUtil.getInstance().strings().set("kafka_test_0001","这个这只是一个测试");

        String testValue = JRedisUtil.getInstance().strings().get("kafka_test_0001");

        ConfigHelper.getProperty("");
        Properties props = new Properties();
        // 定义kakfa 服务的地址，不需要将所有broker指定上
        props.put("bootstrap.servers", "master01:9092,slave02:9092,slave03:9092");
    }
}
