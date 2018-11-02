import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Properties;

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

            for (ConsumerRecord<String, String> record : records) {
                String[] array = record.value().toString().split(",");

                String id = array[0].replace("\"", "");
                int questionid = Integer.parseInt(array[1].replace("\"", ""));
                int pointid = Integer.parseInt(array[2].replace("\"", ""));
                String rowkey = questionid+"_"+pointid;
                System.out.println(rowkey);
//                Rowkey:  questionid + pointid
                //添加指定的rowkey的数据，和mysql保持同步
                Kafka_hbase.insertData("t_question_point","qp",rowkey,"coeffcient","0");

                //删除指定的rowkey的数据，和mysql保持同步
//                Kafka_hbase.deleteRow("t_answerrecord",rowkey);
                //修改指定的rowkey的数据，和mysql同步
//                Kafka_hbase.updateData("t_answerrecord","p",rowkey,pointid,isright);
                System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
            }
        }
    }

}
