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
                String recordValue = record.value().toString().replace("\"", "");
                String[] array = record.value().toString().split(",");
                if (array.length < 2) {
                    continue;
                }
                String id = array[0];
                Long testpaper_user_id = Long.valueOf(array[1]);
                Long question_id = Long.valueOf(array[2]);
                Long course_id = Long.valueOf(array[3]);
                Long testpaper_id = Long.valueOf(array[4]);
                Long user_id = Long.valueOf(array[5]);
                int isright = Integer.valueOf(array[6]);

                System.out.println(id);
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
                /*System.out.println(array[1].toString()=="  ");
                System.out.println(array[1].toString()==null);
                System.out.println(array[1].toString().length());
                System.out.println("  ".equals(array[1].toString()));*/

                HBaseUtils hBaseUtils = new HBaseUtils();
                //ResultScanner results = Kafka_hbase.QueryByCondition1("t_question_point", question_id+"_" );
                ResultScanner point_results = null;
                try {
                    point_results = hBaseUtils.PrefixFilter(hBase_questionpoint_table, question_id + "_");
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

                List<Integer> existsPointList = new ArrayList<>();
                for (Result result : point_results) {
                    String question_point_rowkey = new String(result.getRow());
                    String[] strings = question_point_rowkey.split("_");
                    int pointid = Integer.parseInt(strings[1].toString());

                    existsPointList.add(pointid);

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
                            p_f_count += 1;
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
                    Put q_count_put = new Put(Bytes.toBytes(rowkey));
                    q_count_put.addColumn(Bytes.toBytes(p_family), Bytes.toBytes("q_count"), Bytes.toBytes(q_count));
                    updateCells.add(q_count_put);

                    Put q_r_count_put = new Put(Bytes.toBytes(rowkey));
                    q_r_count_put.addColumn(Bytes.toBytes(p_family), Bytes.toBytes("q_r_count"), Bytes.toBytes(q_r_count));
                    updateCells.add(q_r_count_put);

                    Put q_f_count_put = new Put(Bytes.toBytes(rowkey));
                    q_f_count_put.addColumn(Bytes.toBytes(p_family), Bytes.toBytes("q_f_count"), Bytes.toBytes(q_f_count));
                    updateCells.add(q_f_count_put);

                    Put p_count_put = new Put(Bytes.toBytes(rowkey));
                    p_count_put.addColumn(Bytes.toBytes(p_family), Bytes.toBytes("p_count"), Bytes.toBytes(p_count));
                    updateCells.add(p_count_put);

                    Put p_r_count_put = new Put(Bytes.toBytes(rowkey));
                    p_r_count_put.addColumn(Bytes.toBytes(p_family), Bytes.toBytes("p_r_count"), Bytes.toBytes(p_r_count));
                    updateCells.add(p_r_count_put);

                    Put p_f_count_put = new Put(Bytes.toBytes(rowkey));
                    p_f_count_put.addColumn(Bytes.toBytes(p_family), Bytes.toBytes("p_f_count"), Bytes.toBytes(p_f_count));
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
                    Integer numCellValue = 1;
                    for (HBaseCellModel hBaseCellModel : questionCellModels) {
                        if (q_columns.equals(hBaseCellModel.Quilifier) || q_columns_fix.equals(hBaseCellModel.Quilifier) ) {
                            numCellValue = Integer.valueOf(hBaseCellModel.CellValue) + numCellValue;
                            Put put = new Put(Bytes.toBytes(rowkey));
                            put.addColumn(Bytes.toBytes(hBaseCellModel.Family), Bytes.toBytes(hBaseCellModel.Quilifier), Bytes.toBytes(numCellValue));
                            updateCells.add(put);

                            isExistsQuestion = true;
                        }
                    }
                    if(isExistsQuestion ==false){
                        Put put = new Put(Bytes.toBytes(rowkey));
                        put.addColumn(Bytes.toBytes(p_family), Bytes.toBytes(q_columns), Bytes.toBytes(numCellValue));
                        updateCells.add(put);

                        Put put_fix = new Put(Bytes.toBytes(rowkey));
                        put_fix.addColumn(Bytes.toBytes(p_family), Bytes.toBytes(q_columns_fix), Bytes.toBytes(numCellValue));
                        updateCells.add(put_fix);
                    }
                }

                {
                    for (Integer pointid : existsPointList) {
                        Integer numCellValue = 1;
                        String p_family = "p";
                        String p_quilifier = pointid.toString();
                        boolean isExists = false;
                        if (isright == 0) {
                            p_quilifier = p_quilifier + "_0";
                        } else {
                            p_quilifier = p_quilifier + "_1";
                        }
                        for (HBaseCellModel hBaseCellModel : pointCellModels) {
                            if (hBaseCellModel.Quilifier.equals(pointid) || hBaseCellModel.Quilifier.equals(p_quilifier)) {
                                numCellValue = Integer.valueOf(hBaseCellModel.CellValue) + numCellValue;
                                Put put = new Put(Bytes.toBytes(rowkey));
                                put.addColumn(Bytes.toBytes(hBaseCellModel.Family), Bytes.toBytes(hBaseCellModel.Quilifier), Bytes.toBytes(numCellValue));
                                updateCells.add(put);
                                isExists = true;
                                break;
                            }
                        }
                        if (isExists == false) {

                            Put put = new Put(Bytes.toBytes(rowkey));
                            put.addColumn(Bytes.toBytes(p_family), Bytes.toBytes(pointid.toString()), Bytes.toBytes(numCellValue));
                            updateCells.add(put);

                            Put put_fix = new Put(Bytes.toBytes(rowkey));
                            put_fix.addColumn(Bytes.toBytes(p_family), Bytes.toBytes(p_quilifier), Bytes.toBytes(numCellValue));
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
