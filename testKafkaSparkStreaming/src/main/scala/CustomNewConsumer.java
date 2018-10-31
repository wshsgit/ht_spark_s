import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class CustomNewConsumer {

    public static void main(String[] args) {
//        Kafka_hbase.insertData("t_answerrecord");
        Properties props = new Properties();
        // 定义kakfa 服务的地址，不需要将所有broker指定上
        props.put("bootstrap.servers", "master01:9092,slave02:9092,slave03:9092");
        // 制定consumer group
        props.put("group.id", "answerrecord");
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
        consumer.subscribe(Arrays.asList("tzanswerrecord"));

        while (true) {
            // 读取数据，读取超时时间为100ms
            ConsumerRecords<String, String> records = consumer.poll(100);

            for (ConsumerRecord<String, String> record : records) {
                String[] array = record.value().toString().split(",");

                String id = array[0];
                String subjectid = array[1];
                String testpaperid = array[2];
                String userid = array[3];
                String questionid = array[4];
                String subtime = array[5];
                String pointid = array[6];
                String isright = array[7];
                String rowkey = id+"_"+subjectid+"_"+testpaperid+"_"+userid+"_"+questionid+"_"+subtime;//-->subtime
                System.out.println(rowkey);
//                Rowkey: subjectid + testpaperid + userid + questionid + createtime
                //添加指定的rowkey的数据，和mysql保持同步
                Kafka_hbase.insertData("t_answerrecord","p",rowkey,pointid,isright);

                //删除指定的rowkey的数据，和mysql保持同步
//                Kafka_hbase.deleteRow("t_answerrecord",rowkey);
                //修改指定的rowkey的数据，和mysql同步
//                Kafka_hbase.updateData("t_answerrecord","p",rowkey,pointid,isright);
                System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
            }
        }
    }

}
