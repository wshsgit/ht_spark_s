package com.ht.kafka.paperreport.business;

import com.ht.utils.HBaseUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.*;

public class KafkaProcessSummary {
    public static void main(String[] args) {
        //SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Properties props = new Properties();
        // 定义kakfa 服务的地址，不需要将所有broker指定上
        props.put("bootstrap.servers", "master01:9092,slave02:9092,slave03:9092");
        //props.put("bootstrap.servers", "59.110.216.70:9092,47.95.1.29:9092,59.110.166.163:9092");
        // 制定consumer group
        props.put("group.id", "user_paper_summary_01");
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
        consumer.subscribe(Arrays.asList("tzuser_paper_summary"));
        Logger logger = LoggerFactory.getLogger(KafkaProcessSummary.class);
        HashMap<Long,List<Integer>> question_point_map = new HashMap<>();
        HashMap<Long,Long> paper_customs_map = new HashMap<>();
        while (true) {
            // 读取数据，读取超时时间为100ms
            ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord<String, String> record : records) {
                Customs_Single_Processer processer = null;
                try {
                    processer = new Customs_Single_Processer(record,question_point_map,paper_customs_map);
                } catch (Exception e) {
                    e.printStackTrace();
                    continue;
                }
                processer.Process_PaperSummary();
                processer.Process_Customs();
                System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
                logger.info("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
            }
        }
    }
}
