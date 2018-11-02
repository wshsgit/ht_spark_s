
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;
import scala.Int;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class Kafka_hbase {

    public static Configuration configuration;

    static {
        configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.property.clientPort", "2181");
        configuration.set("hbase.zookeeper.quorum", "master01,slave02,slave03");
    }

    public static void main(String[] args) {
          /*tablename:t_answerrecord:
      Rowkey: subjectid + testpaperid + userid + questionid + createtime
      ColumnFamily:p
       pointid值为具体的列名*/
//        createTable("t_question_point");
//        createTable("t_answerrecord");
//         insertData("t_answerrecord","p","123456","point","0");
        QueryAll("t_question_point");
//         QueryByCondition1("t_question_point","\"1001\"_");
//        QueryByRowkey("t_question_point", "\"1001\"_", "coeffcient");
//        QueryByCondition2("t_answerrecord");
//        QueryByCondition3("t_answerrecord");
//        deleteRow("test","row1");
        // deleteByCondition("test","aaa");
        //dropTable("test1");
    }


    /**
     * 创建表
     *
     * @param tableName
     */
    public static void createTable(String tableName) {
        System.out.println("start create table ......");
        try {
            HBaseAdmin hBaseAdmin = null;
            try {
                hBaseAdmin = new HBaseAdmin(configuration);
            } catch (IOException e) {
                e.printStackTrace();
            }
            if (hBaseAdmin.tableExists(tableName)) {// 如果存在要创建的表，那么先删除，再创建
                hBaseAdmin.disableTable(tableName);
                hBaseAdmin.deleteTable(tableName);
                System.out.println(tableName + " is exist,detele....");
            }
            HTableDescriptor tableDescriptor = new HTableDescriptor(tableName);
            tableDescriptor.addFamily(new HColumnDescriptor("qp"));
//            tableDescriptor.addFamily(new HColumnDescriptor("q"));
//            tableDescriptor.addFamily(new HColumnDescriptor("p"));

            hBaseAdmin.createTable(tableDescriptor);
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.out.println("end create table ......");
    }

    /**
     * 插入数据
     *
     * @param tableName
     */
    public static void insertData(String tableName, String cf, String rowkey, String col, String value) {
        System.out.println("start insert data ......");
        HTablePool pool = new HTablePool(configuration, 1000);
        Put put = new Put(rowkey.getBytes());// 一个PUT代表一行数据，再NEW一个PUT表示第二行数据,每行一个唯一的ROWKEY，此处rowkey为put构造方法中传入的值
        put.add(cf.getBytes(), col.getBytes(), value.getBytes());// 本行数据的第一列
        try {
            //如今应用的api版本中pool.getTable返回的类型是HTableInterface ，无法强转为HTable
            pool.getTable(tableName).put(put);
        } catch (Exception e) {

        }
        System.out.println("end insert data ......");

    }

    /**
     * 插入sum数据
     *
     * @param tableName
     */
    public static void insertDataSum(String tableName, String rowkey, String s, String q_count, String q_count_value, String q_r_count, String q_r_count_value,
                                     String p_count, String p_count_value, String p_r_count, String p_r_count_value,
                                     String q, String qid_0, String qid_0_value, String qid, String qid_value,
                                     String p, String pid_0, String pid_0_value, String pid, String pid_value) {
        System.out.println("start insert data ......");
        HTablePool pool = new HTablePool(configuration, 1000);
        Put put = new Put(rowkey.getBytes());// 一个PUT代表一行数据，再NEW一个PUT表示第二行数据,每行一个唯一的ROWKEY，此处rowkey为put构造方法中传入的值
        put.add(s.getBytes(), q_count.getBytes(), q_count_value.getBytes());// 本行数据的第一列
        put.add(s.getBytes(), q_r_count.getBytes(), q_r_count_value.getBytes());// 本行数据的第二列
        put.add(s.getBytes(), p_count.getBytes(), p_count_value.getBytes());// 本行数据的第一列
        put.add(s.getBytes(), p_r_count.getBytes(), p_r_count_value.getBytes());// 本行数据的第二列
        put.add(q.getBytes(), qid_0.getBytes(), qid_0_value.getBytes());// 本行数据的第一列
        put.add(q.getBytes(), qid.getBytes(), qid_value.getBytes());// 本行数据的第二列
        put.add(p.getBytes(), pid_0.getBytes(), pid_0_value.getBytes());// 本行数据的第一列
        put.add(p.getBytes(), pid.getBytes(), pid_value.getBytes());// 本行数据的第二列
        try {
            //如今应用的api版本中pool.getTable返回的类型是HTableInterface ，无法强转为HTable
            pool.getTable(tableName).put(put);
        } catch (Exception e) {

        }
        System.out.println("end insert data ......");

    }

    /**
     * 修改数据
     *
     * @param tableName
     */
    public static void updateData(String tableName, String cf, String rowkey, String col, String value) {
        System.out.println("start update data ......");
        HTablePool pool = new HTablePool(configuration, 1000);
        Put put = new Put(rowkey.getBytes());// 一个PUT代表一行数据，再NEW一个PUT表示第二行数据,每行一个唯一的ROWKEY，此处rowkey为put构造方法中传入的值
        put.add(cf.getBytes(), col.getBytes(), value.getBytes());// 本行数据的第一列
        try {
            //如今应用的api版本中pool.getTable返回的类型是HTableInterface ，无法强转为HTable
            pool.getTable(tableName).put(put);
        } catch (Exception e) {

        }
        System.out.println("end update data ......");

    }

    /**
     * 删除一张表
     *
     * @param tableName
     */
    public static void dropTable(String tableName) {
        try {
            HBaseAdmin admin = new HBaseAdmin(configuration);
            admin.disableTable(tableName);
            admin.deleteTable(tableName);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 根据 rowkey删除一条记录
     *
     * @param tablename
     * @param rowkey
     */
    public static void deleteRow(String tablename, String rowkey) {
        try {
            HTable table = new HTable(configuration, tablename);
            List list = new ArrayList();
            Delete d1 = new Delete(rowkey.getBytes());
            list.add(d1);

            table.delete(list);
            System.out.println("删除行成功!");

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 组合条件删除
     *
     * @param tablename
     * @param rowkey
     */
    public static void deleteByCondition(String tablename, String rowkey) {
        //目前还没有发现有效的API能够实现 根据非rowkey的条件删除 这个功能能，还有清空表全部数据的API操作
    }

    /**
     * 查询所有数据
     *
     * @param tableName
     */
    public static void QueryAll(String tableName) {
        HTablePool pool = new HTablePool(configuration, 1000);

        try {
            //如今应用的api版本中pool.getTable返回的类型是HTableInterface ，无法强转为HTable
            ResultScanner rs = pool.getTable(tableName).getScanner(new Scan());
            for (Result r : rs) {
                System.out.println("获得到rowkey:" + new String(r.getRow()));
                for (KeyValue keyValue : r.raw()) {
                    System.out.println("列：" + new String(keyValue.getFamily())
                            + "====值:" + new String(keyValue.getValue()));
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 单条件查询,根据rowkey查询唯一一条记录
     *
     * @param tableName
     */
    public static ResultScanner QueryByCondition1(String tableName, String rowKet) throws IOException {

        HTablePool pool = new HTablePool(configuration, 1000);
        ResultScanner results=null;
        Scan scan = new Scan();
        try {
//            Get scan2 = new Get(rowKet.getBytes());// 根据rowkey查询
            //如今应用的api版本中pool.getTable返回的类型是HTableInterface ，无法强转为HTable
            PrefixFilter filter = new PrefixFilter(Bytes.toBytes(rowKet));
            scan.setFilter(filter);
            results = pool.getTable(tableName).getScanner(scan);

//            System.out.println("获得到rowkey:" + results.toString());
//            for (Result result : results) {
//                System.out.println("获得到rowkey:" + new String(result.getRow()));
//                for (KeyValue keyValue : result.raw()) {
//                    System.out.println("列族：" + new String(keyValue.getFamily()) + "列" + new String(keyValue.getQualifier())
//                            + "====值:" + new String(keyValue.getValue()));
//                }
//
//            }
            return  results;
        } catch (IOException e) {
            e.printStackTrace();
        }

        return  results;
    }

    /**
     * 单条件查询,根据rowkey查询唯一一条记录
     *
     * @param tableName
     */
    public static int QueryByRowkey(String tableName, String rowKey, String lie) {

        HTablePool pool = new HTablePool(configuration, 1000);
        int h_value = 0;
        try {
            FilterList filterList = new FilterList();

            RowFilter filter1 = new RowFilter(CompareFilter.CompareOp.LESS_OR_EQUAL, new BinaryComparator(Bytes.toBytes(rowKey)));
            filterList.addFilter(filter1);
            Filter filter2 = new QualifierFilter(CompareFilter.CompareOp.LESS_OR_EQUAL, new BinaryComparator(Bytes.toBytes(lie)));
            filterList.addFilter(filter2);

            Scan scan = new Scan();
            scan.setFilter(filterList);
//            Get scan = new Get(rowKey.getBytes());// 根据rowkey查询
            //如今应用的api版本中pool.getTable返回的类型是HTableInterface ，无法强转为HTable
            ResultScanner resultScanner = pool.getTable(tableName).getScanner(scan);

//            System.out.println("获得到rowkey:" + results.toString());
            for (Result result : resultScanner) {
                System.out.println("获得到rowkey:" + new String(result.getRow()));
                for (KeyValue keyValue : result.raw()) {
                    System.out.println("列：" + new String(keyValue.getFamily())
                            + "====值:" + new String(keyValue.getValue()));
                    String value = new String(keyValue.getValue());

                    if (value == null || "".equals(value)) {
                        value = "0";
                    }
                    h_value = Integer.parseInt(value);
                }
            }
            return h_value;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return h_value;
    }

    /**
     * 单条件按查询，查询多条记录
     *
     * @param tableName
     */
    public static void QueryByCondition2(String tableName) {

        try {
            HTablePool pool = new HTablePool(configuration, 1000);
            Filter filter = new SingleColumnValueFilter(Bytes.toBytes("column1"), null, CompareFilter.CompareOp.EQUAL,
                    Bytes.toBytes("bbb")); // 当列column1的值为aaa时进行查询
            Scan s = new Scan();
            s.setFilter(filter);
            ResultScanner rs = pool.getTable(tableName).getScanner(s);
            for (Result r : rs) {
                System.out.println("获得到rowkey:" + new String(r.getRow()));
                for (KeyValue keyValue : r.raw()) {
                    System.out.println("列族： " + new String(keyValue.getFamily()) + "==>,列：" + new String(keyValue.getKey())
                            + "====值:" + new String(keyValue.getValue()));
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    /**
     * 组合条件查询
     *
     * @param tableName
     */
    public static void QueryByCondition3(String tableName) {

        try {
            HTablePool pool = new HTablePool(configuration, 1000);

            List<Filter> filters = new ArrayList<Filter>();

            Filter filter1 = new SingleColumnValueFilter(Bytes
                    .toBytes("column1"), null, CompareFilter.CompareOp.EQUAL, Bytes
                    .toBytes("aaa"));
            filters.add(filter1);

            Filter filter2 = new SingleColumnValueFilter(Bytes
                    .toBytes("column2"), null, CompareFilter.CompareOp.EQUAL, Bytes
                    .toBytes("bbb"));
            filters.add(filter2);

            Filter filter3 = new SingleColumnValueFilter(Bytes
                    .toBytes("column3"), null, CompareFilter.CompareOp.EQUAL, Bytes
                    .toBytes("ccc"));
            filters.add(filter3);

            FilterList filterList1 = new FilterList(filters);

            Scan scan = new Scan();
            scan.setFilter(filterList1);
            ResultScanner rs = pool.getTable(tableName).getScanner(scan);
            for (Result r : rs) {
                System.out.println("获得到rowkey:" + new String(r.getRow()));
                for (KeyValue keyValue : r.raw()) {
                    System.out.println("列：" + new String(keyValue.getFamily())
                            + "====值:" + new String(keyValue.getValue()));
                }
            }
            rs.close();

        } catch (Exception e) {
            e.printStackTrace();
        }

    }


}
