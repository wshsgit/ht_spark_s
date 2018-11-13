package com.ht.kafka.paperreport.test;

import com.ht.utils.JRedisUtil;
import com.ht.utils.ConfigHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class Test {
    public static void main(String[] args) {
        System.out.printf("Test.Main");
        JRedisUtil.getInstance().strings().set("kafka_test_0001","这个这只是一个测试");

        String testValue = JRedisUtil.getInstance().strings().get("kafka_test_0001");
        System.out.printf("Test.Main,key:"+testValue);
        Logger logger = LoggerFactory.getLogger(Test.class);
        logger.info("key："+ "kafka_test_0001,value:"+ testValue);
        /*ConfigHelper.getProperty("");
        Properties props = new Properties();
        // 定义kakfa 服务的地址，不需要将所有broker指定上
        props.put("bootstrap.servers", "master01:9092,slave02:9092,slave03:9092");*/
    }
}
