package com.ht.kafka.paperreport.business;

import com.alibaba.fastjson.JSONObject;
import com.ht.utils.HBaseUtils;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

/***
 * 处理试卷和Consumer业务的关系
 */
public class CustomsTaskUserDetailConsumer {
    public static void main(String[] args) {
        Properties props = new Properties();
        // 定义kakfa 服务的地址，不需要将所有broker指定上
        props.put("bootstrap.servers", "master01:9092,slave02:9092,slave03:9092");

        // 制定consumer group
        props.put("group.id", "topic_customs_task_user_detail");
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
        consumer.subscribe(Arrays.asList("topic_customs_task_user_detail"));

        while (true) {
            // 读取数据，读取超时时间为100ms
            ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord<String, String> record : records) {
                JSONObject jsonObject = JSONObject.parseObject(record.value());

                Long id = jsonObject.getLong("id");
                Long testPaper_user_id = jsonObject.getLong("testPaper_user_id");
                Long customsID = jsonObject.getLong("CustomsID");
                Long taskID = jsonObject.getLong("TaskID");
                Long userID = jsonObject.getLong("UserID");
                String rowkey = testPaper_user_id + "_" + taskID + "_" + customsID;

                List<Put> puts = new ArrayList<>();
                Put put_Task = new Put(Bytes.toBytes(rowkey));
                put_Task.addColumn(Bytes.toBytes("i"), Bytes.toBytes("TaskID"), Bytes.toBytes(String.valueOf(String.valueOf(taskID))));
                puts.add(put_Task);

                Put put_UserID = new Put(Bytes.toBytes(rowkey));
                put_UserID.addColumn(Bytes.toBytes("i"), Bytes.toBytes("UserID"), Bytes.toBytes(String.valueOf(String.valueOf(userID))));
                puts.add(put_UserID);
                try {
                    HBaseUtils.PutList("t_customs_task_user_detail",puts);
                } catch (Exception e) {
                    e.printStackTrace();
                }

                System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
            }
        }
    }

}
