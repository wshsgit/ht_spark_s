package com.ht.kafka.paperreport.business;

import com.alibaba.fastjson.JSONObject;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import com.ht.utils.HBaseUtils;

public class QuestionPointConsumer {
    public static void main(String[] args) {
        Properties props = new Properties();
        // 定义kakfa 服务的地址，不需要将所有broker指定上
        props.put("bootstrap.servers", "master01:9092,slave02:9092,slave03:9092");

        // 制定consumer group
        props.put("group.id", "questionpoint");
        // 是否自动确认offset
        props.put("enable.auto.commit", "true");
        // 自动确认offset的时间间隔
        props.put("auto.commit.interval.ms", "1000");
        // key的序列化类
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        // value的序列化类
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        // 定义consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

        // 指定要消费的topic, 可同时处理多个
        consumer.subscribe(Arrays.asList("tzquestionpoint"));

        while (true) {
            // 读取数据，读取超时时间为100ms
            ConsumerRecords<String, String> records = consumer.poll(100);
            HBaseUtils hBaseUtils = new HBaseUtils();
            for (ConsumerRecord<String, String> record : records) {
                JSONObject jsonObject = JSONObject.parseObject(record.value());

                Long id = jsonObject.getLong("id");
                Long questionid = jsonObject.getLong("QuestionId");
                Long pointid = jsonObject.getLong("PointId");
                String rowkey = questionid + "_" + pointid;

                List<Put> puts = new ArrayList<>();
                Put put = new Put(Bytes.toBytes(rowkey));
                put.addColumn(Bytes.toBytes("qp"), Bytes.toBytes("coeffcient"), Bytes.toBytes(String.valueOf(String.valueOf(0))));
                puts.add(put);
                try {
                    hBaseUtils.PutList("t_question_point",puts);
                } catch (Exception e) {
                    e.printStackTrace();
                }

                System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
            }
        }
    }
}
