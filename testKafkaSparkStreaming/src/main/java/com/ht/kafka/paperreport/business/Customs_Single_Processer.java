package com.ht.kafka.paperreport.business;

import com.alibaba.fastjson.JSONObject;
import com.ht.entity.HBaseCellModel;
import com.ht.utils.HBaseUtils;
import com.ht.utils.JRedisUtil;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.*;

/***
 * 一次处理单条记录，后续优化为处理多条记录，即合并多条记录一次写入
 */
public class Customs_Single_Processer {
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

    long id;
    String testpaper_user_id;
    String question_id;
    String user_id;
    int isright;

    HashMap<Long,List<Integer>> question_point_map = new HashMap<>();
    HashMap<Long,Long> paper_customs_map = new HashMap<>();
    List<Integer> current_question_points = question_point_map.get(question_id);

    public Customs_Single_Processer(ConsumerRecord<String, String> record, HashMap<Long,List<Integer>> question_point_map,HashMap<Long,Long> paper_customs_map) throws Exception {
        //id,testpaperuser_id,questionid,courseid,paperid,userid,
        JSONObject jsonObject = JSONObject.parseObject(record.value());

        id = jsonObject.getLong("id");
        Long testPaper_User_ID = jsonObject.getLong("TestPaper_User_ID");
        Long questionID = jsonObject.getLong("QuestionID");
        Long course_id = jsonObject.getLong("BatchCourses_ID");
        Long testpaper_id = jsonObject.getLong("TestPaper_ID");
        Long createUser = jsonObject.getLong("CreateUser");
        Integer isRight = jsonObject.getInteger("IsRight");

        this.paper_customs_map = paper_customs_map;
        this.question_point_map = question_point_map;

        if(isRight == null||testPaper_User_ID == null||questionID==null||createUser == null){
            throw new Exception("消息错误，待处理");
        }
        else {
            testpaper_user_id = testPaper_User_ID.toString();
            question_id = questionID.toString();
            user_id = createUser.toString();
            isright = isRight;
        }
        current_question_points = question_point_map.get(questionID);
        if (current_question_points==null||current_question_points.size()==0) {
            ResultScanner current_question_points_result = null;
            try {
                String hBase_questionpoint_table = "t_question_point";
                current_question_points_result = HBaseUtils.PrefixFilter(hBase_questionpoint_table, question_id + "_");
            } catch (Exception e) {
                e.printStackTrace();
                return;
            }

            current_question_points = new ArrayList<>();
            if (current_question_points_result !=null) {
                for (Result result : current_question_points_result) {
                    String question_point_rowkey = new String(result.getRow());
                    String[] strings = question_point_rowkey.split("_");
                    Integer pointid = Integer.parseInt(strings[1].toString());

                    current_question_points.add(pointid);
                }
                try{
                    current_question_points_result.close();
                }catch (Exception e){

                }
            }
            if (current_question_points.size() >0) {
                question_point_map.put(Long.valueOf(question_id), current_question_points);
            }
        }

    }


    public void Process_PaperSummary()
    {
        String hBase_TableName = "user_paper_summary";
        String rowKey = user_id  + "_" + testpaper_user_id;
        Process_Common(hBase_TableName,rowKey);

        // 排行榜计算 基于redis
        {
            if (isright==1){
                String paperSummaryRank_key = "paperSummaryRank_" + testpaper_user_id;
                double paper_score = JRedisUtil.getInstance().sortSet().zscore( paperSummaryRank_key,user_id.toString());
                if (paper_score<=0){
                    paper_score = 1;
                }
                else{
                    paper_score += 1;
                }
                JRedisUtil.getInstance().sortSet().zadd(paperSummaryRank_key, paper_score, user_id.toString());
            }
        }


    }

    public void Process_Customs(){
        Long customs_id = paper_customs_map.get(testpaper_user_id);
        if (customs_id==null){
            //从Hbase里读取

            ResultScanner current_paper_customs_result = null;
            try {
                String hBasepaper_customs_table = "t_customs_task_user_detail";
                current_paper_customs_result = HBaseUtils.PrefixFilter(hBasepaper_customs_table, question_id + "_");
            } catch (Exception e) {
                e.printStackTrace();
                return;
            }

            if (current_paper_customs_result!=null) {
                for (Result result : current_paper_customs_result) {
                    String question_point_rowkey = new String(result.getRow());
                    String[] strings = question_point_rowkey.split("_");
                    customs_id = Long.parseLong(strings[1].toString());

                    paper_customs_map.put(Long.parseLong(testpaper_user_id),customs_id);
                }
                try{
                    current_paper_customs_result.close();
                }catch (Exception e){

                }
            }
            if (customs_id!= null && customs_id > 0) {
                paper_customs_map.put(Long.valueOf(testpaper_user_id), customs_id);
                String hBase_TableName = "t_customs_summary";
                String rowKey = user_id  + "_" + customs_id;
                Process_Common(hBase_TableName,rowKey);
            }
        }
        // 排行榜计算 基于redis
        {
            if (isright==1){
                String customsRank_key = "customsRank_" + customs_id;
                double customs_score = JRedisUtil.getInstance().sortSet().zscore( customsRank_key,user_id.toString());
                if (customs_score<=0){
                    customs_score = 1;
                }
                else{
                    customs_score += 1;
                }
                JRedisUtil.getInstance().sortSet().zadd(customsRank_key, customs_score, user_id.toString());
            }
        }
    }

    private void Process_Common(String hBase_TableName,String rowKey)
    {
        Result paperSummaryResult = null;
        try {
            paperSummaryResult = HBaseUtils.GetByRowKey(hBase_TableName, rowKey);
        } catch (Exception e) {
            e.printStackTrace();
            return;
        }
        List<Put> updateCells = new ArrayList<>();
        List<HBaseCellModel> pointCellModels = new ArrayList<>();
        List<HBaseCellModel> questionCellModels = new ArrayList<>();
        List<HBaseCellModel> summaryCellModels = new ArrayList<>();
        if (paperSummaryResult ==null){
            return;//打出日志
        }
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



            Put q_count_put = new Put(Bytes.toBytes(rowKey));
            q_count_put.addColumn(Bytes.toBytes(p_family), Bytes.toBytes("q_count"), Bytes.toBytes(String.valueOf(q_count)));
            updateCells.add(q_count_put);

            Put q_r_count_put = new Put(Bytes.toBytes(rowKey));
            q_r_count_put.addColumn(Bytes.toBytes(p_family), Bytes.toBytes("q_r_count"), Bytes.toBytes(String.valueOf(q_r_count)));
            updateCells.add(q_r_count_put);

            Put q_f_count_put = new Put(Bytes.toBytes(rowKey));
            q_f_count_put.addColumn(Bytes.toBytes(p_family), Bytes.toBytes("q_f_count"), Bytes.toBytes(String.valueOf(q_f_count)));
            updateCells.add(q_f_count_put);

            Put p_count_put = new Put(Bytes.toBytes(rowKey));
            p_count_put.addColumn(Bytes.toBytes(p_family), Bytes.toBytes("p_count"), Bytes.toBytes(String.valueOf(p_count)));
            updateCells.add(p_count_put);

            Put p_r_count_put = new Put(Bytes.toBytes(rowKey));
            p_r_count_put.addColumn(Bytes.toBytes(p_family), Bytes.toBytes("p_r_count"), Bytes.toBytes(String.valueOf(p_r_count)));
            updateCells.add(p_r_count_put);

            Put p_f_count_put = new Put(Bytes.toBytes(rowKey));
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
                    Put put = new Put(Bytes.toBytes(rowKey));
                    put.addColumn(Bytes.toBytes(hBaseCellModel.Family), Bytes.toBytes(hBaseCellModel.Quilifier), Bytes.toBytes(String.valueOf(numCellValue)));
                    updateCells.add(put);

                    isExistsQuestion = true;
                }
            }
            if(isExistsQuestion ==false){
                Put put = new Put(Bytes.toBytes(rowKey));
                put.addColumn(Bytes.toBytes(p_family), Bytes.toBytes(q_columns), Bytes.toBytes(String.valueOf(1)));
                updateCells.add(put);

                Put put_fix = new Put(Bytes.toBytes(rowKey));
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
                        Put put = new Put(Bytes.toBytes(rowKey));
                        put.addColumn(Bytes.toBytes(hBaseCellModel.Family), Bytes.toBytes(hBaseCellModel.Quilifier), Bytes.toBytes(String.valueOf(numCellValue)));
                        updateCells.add(put);
                        isExists = true;
                        break;
                    }
                }
                if (isExists == false) {

                    Put put = new Put(Bytes.toBytes(rowKey));
                    put.addColumn(Bytes.toBytes(p_family), Bytes.toBytes(pointid.toString()), Bytes.toBytes(String.valueOf(1)));
                    updateCells.add(put);

                    Put put_fix = new Put(Bytes.toBytes(rowKey));
                    put_fix.addColumn(Bytes.toBytes(p_family), Bytes.toBytes(p_quilifier), Bytes.toBytes(String.valueOf(1)));
                    updateCells.add(put_fix);
                }
            }
        }

        try {
            HBaseUtils.PutList(hBase_TableName,updateCells);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
