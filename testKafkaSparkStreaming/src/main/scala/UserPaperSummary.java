import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.hadoop.hbase.client.Put;
import com.alibaba.fastjson.JSONObject;
import scala.Int;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.*;

public class UserPaperSummary {

    public static void main(String[] args) {
//        Kafka_hbase.insertData("t_answerrecord");
//        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Properties props = new Properties();
        // 定义kakfa 服务的地址，不需要将所有broker指定上
        props.put("bootstrap.servers", "master01:9092,slave02:9092,slave03:9092");
        //props.put("bootstrap.servers", "59.110.216.70:9092,47.95.1.29:9092,59.110.166.163:9092");
        // 制定consumer group
        props.put("group.id", "user_paper_summary");
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

        String hBase_summaryPaper_table = "user_paper_summary";
        String hBase_questionpoint_table = "t_question_point";
        // 指定要消费的topic, 可同时处理多个
        consumer.subscribe(Arrays.asList("tzuser_paper_summary"));

        while (true) {
            // 读取数据，读取超时时间为100ms
            ConsumerRecords<String, String> records = consumer.poll(100);

            for (ConsumerRecord<String, String> record : records) {

                //id,testpaperuser_id,questionid,courseid,paperid,userid,
                JSONObject jsonObject = JSONObject.parseObject(record.value());

                Long id = jsonObject.getLong("id");
                Long testpaper_user_id = jsonObject.getLong("TestPaper_User_ID");
                Long question_id = jsonObject.getLong("QuestionID");
                Long course_id = jsonObject.getLong("BatchCourses_ID");
                Long testpaper_id = jsonObject.getLong("TestPaper_ID");
                Long user_id = jsonObject.getLong("CreateUser");
                Integer isright = jsonObject.getInteger("IsRight");

                if(isright == null||testpaper_user_id == null){
                    continue;
                }

                //System.out.println(id);int isright = Integer.valueOf(array[6]);

                /***
                 * 试卷总题数（不含重复）
                 */
                int q_count = 0;
                /***
                 * 作对题数
                 */
                int q_r_count = 0;
                /***
                 * 做错题数
                 */
                int q_f_count = 0;
                /***
                 * 做过知识点数（不含重复）
                 */
                int p_count = 0;
                /***
                 * 作对知识点数
                 */
                int p_r_count = 0;
                /***
                 * 做错知识点数
                 */
                int p_f_count = 0;

                HBaseUtils hBaseUtils = new HBaseUtils();
                ResultScanner current_question_points_result = null;
                try {
                    current_question_points_result = hBaseUtils.PrefixFilter(hBase_questionpoint_table, question_id + "_");
                } catch (Exception e) {
                    e.printStackTrace();
                    continue;
                }
                String rowkey = user_id + "_" + testpaper_user_id;

                Result paperSummaryResult = null;
                try {
                    paperSummaryResult = hBaseUtils.GetByRowKey(hBase_summaryPaper_table, rowkey);
                } catch (Exception e) {
                    e.printStackTrace();
                    continue;
                }

                List<Integer> current_question_points = new ArrayList<>();
                for (Result result : current_question_points_result) {
                    String question_point_rowkey = new String(result.getRow());
                    String[] strings = question_point_rowkey.split("_");
                    int pointid = Integer.parseInt(strings[1].toString());

                    current_question_points.add(pointid);

                }

                List<Put> updateCells = new ArrayList<>();
                List<HBaseCellModel> pointCellModels = new ArrayList<>();
                List<HBaseCellModel> questionCellModels = new ArrayList<>();
                List<HBaseCellModel> summaryCellModels = new ArrayList<>();
                for (Cell cell : paperSummaryResult.rawCells()) {
                    String quilifier = Bytes.toString(CellUtil.cloneQualifier(cell));
                    String family = Bytes.toString(CellUtil.cloneFamily(cell));
                    String cellValue = Bytes.toString(CellUtil.cloneValue(cell));
                    String tmpRowKey = Bytes.toString(CellUtil.cloneRow(cell));

                    HBaseCellModel hBaseCellModel = new HBaseCellModel();
                    hBaseCellModel.RowKey = tmpRowKey;
                    hBaseCellModel.Family = family;
                    hBaseCellModel.Quilifier = quilifier;
                    hBaseCellModel.CellValue = cellValue;

                    if (family.equals("s")) {
                        summaryCellModels.add(hBaseCellModel);
                    }
                    if (family.equals("p")) {
                        if (quilifier.endsWith("_0")) {
                            p_f_count +=  1;
                        } else if (quilifier.endsWith("_1")) {
                            p_r_count += 1;
                        } else {
                            p_count += 1;
                        }
                        pointCellModels.add(hBaseCellModel);
                    } else if (family.equals("q")) {
                        if (quilifier.endsWith("_0")) {
                            q_f_count += 1;
                        } else if (quilifier.endsWith("_1")) {
                            q_r_count += 1;
                        } else {
                            q_count += 1;
                        }
                        questionCellModels.add(hBaseCellModel);
                    }
                }

                {
                    String p_family = "s";

                    q_count +=1;//加上本题
                    p_count += current_question_points.size();//这里没有剔除当前知识点和已做过的重复问题，后续完善代码
                    if(isright==1){
                        q_r_count +=1;
                        p_r_count += current_question_points.size();
                    }else{
                        q_f_count += 1;
                        p_f_count += current_question_points.size();
                    }



                    Put q_count_put = new Put(Bytes.toBytes(rowkey));
                    q_count_put.addColumn(Bytes.toBytes(p_family), Bytes.toBytes("q_count"), Bytes.toBytes(String.valueOf(q_count)));
                    updateCells.add(q_count_put);

                    Put q_r_count_put = new Put(Bytes.toBytes(rowkey));
                    q_r_count_put.addColumn(Bytes.toBytes(p_family), Bytes.toBytes("q_r_count"), Bytes.toBytes(String.valueOf(q_r_count)));
                    updateCells.add(q_r_count_put);

                    Put q_f_count_put = new Put(Bytes.toBytes(rowkey));
                    q_f_count_put.addColumn(Bytes.toBytes(p_family), Bytes.toBytes("q_f_count"), Bytes.toBytes(String.valueOf(q_f_count)));
                    updateCells.add(q_f_count_put);

                    Put p_count_put = new Put(Bytes.toBytes(rowkey));
                    p_count_put.addColumn(Bytes.toBytes(p_family), Bytes.toBytes("p_count"), Bytes.toBytes(String.valueOf(p_count)));
                    updateCells.add(p_count_put);

                    Put p_r_count_put = new Put(Bytes.toBytes(rowkey));
                    p_r_count_put.addColumn(Bytes.toBytes(p_family), Bytes.toBytes("p_r_count"), Bytes.toBytes(String.valueOf(p_r_count)));
                    updateCells.add(p_r_count_put);

                    Put p_f_count_put = new Put(Bytes.toBytes(rowkey));
                    p_f_count_put.addColumn(Bytes.toBytes(p_family), Bytes.toBytes("p_f_count"), Bytes.toBytes(String.valueOf(p_f_count)));
                    updateCells.add(p_f_count_put);
                }

                {
                    String p_family = "q";
                    String q_columns = question_id.toString();
                    String q_columns_fix = null;
                    if(isright==0){
                        q_columns_fix = question_id + "_0";
                    }else{
                        q_columns_fix = question_id + "_1";
                    }
                    boolean isExistsQuestion = false;
                    for (HBaseCellModel hBaseCellModel : questionCellModels) {
                        if (q_columns.equals(hBaseCellModel.Quilifier) || q_columns_fix.equals(hBaseCellModel.Quilifier) ) {
                            Integer numCellValue = Integer.valueOf(hBaseCellModel.CellValue) + 1;
                            Put put = new Put(Bytes.toBytes(rowkey));
                            put.addColumn(Bytes.toBytes(hBaseCellModel.Family), Bytes.toBytes(hBaseCellModel.Quilifier), Bytes.toBytes(String.valueOf(numCellValue)));
                            updateCells.add(put);

                            isExistsQuestion = true;
                        }
                    }
                    if(isExistsQuestion ==false){
                        Put put = new Put(Bytes.toBytes(rowkey));
                        put.addColumn(Bytes.toBytes(p_family), Bytes.toBytes(q_columns), Bytes.toBytes(String.valueOf(1)));
                        updateCells.add(put);

                        Put put_fix = new Put(Bytes.toBytes(rowkey));
                        put_fix.addColumn(Bytes.toBytes(p_family), Bytes.toBytes(q_columns_fix), Bytes.toBytes(String.valueOf(1)));
                        updateCells.add(put_fix);
                    }
                }

                {
                    for (Integer pointid : current_question_points) {
                        String p_family = "p";
                        String p_quilifier = pointid.toString();
                        boolean isExists = false;
                        if (isright == 0) {
                            p_quilifier = p_quilifier + "_0";
                        } else {
                            p_quilifier = p_quilifier + "_1";
                        }
                        for (HBaseCellModel hBaseCellModel : pointCellModels) {
                            if (hBaseCellModel.Quilifier.equals(pointid.toString()) || hBaseCellModel.Quilifier.equals(p_quilifier)) {
                                Integer numCellValue = Integer.valueOf(hBaseCellModel.CellValue) + 1;
                                Put put = new Put(Bytes.toBytes(rowkey));
                                put.addColumn(Bytes.toBytes(hBaseCellModel.Family), Bytes.toBytes(hBaseCellModel.Quilifier), Bytes.toBytes(String.valueOf(numCellValue)));
                                updateCells.add(put);
                                isExists = true;
                                break;
                            }
                        }
                        if (isExists == false) {

                            Put put = new Put(Bytes.toBytes(rowkey));
                            put.addColumn(Bytes.toBytes(p_family), Bytes.toBytes(pointid.toString()), Bytes.toBytes(String.valueOf(1)));
                            updateCells.add(put);

                            Put put_fix = new Put(Bytes.toBytes(rowkey));
                            put_fix.addColumn(Bytes.toBytes(p_family), Bytes.toBytes(p_quilifier), Bytes.toBytes(String.valueOf(1)));
                            updateCells.add(put_fix);
                        }
                    }

                    System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
                }

                try {
                    hBaseUtils.PutList(hBase_summaryPaper_table,updateCells);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }

}
