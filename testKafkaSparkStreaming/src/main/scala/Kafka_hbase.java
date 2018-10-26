
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
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
//        createTable("t_answerrecord","p");
//         insertData("t_answerrecord","p","123456","point","0");
        //QueryAll("test3");
        // QueryByCondition1("test3","112233bbbcccc");
        QueryByCondition2("t_answerrecord");
//        QueryByCondition3("t_answerrecord");
        //deleteRow("test","row1");
        // deleteByCondition("test","aaa");
        //dropTable("test1");
    }


    /**
     * 创建表
     *
     * @param tableName
     */
    public static void createTable(String tableName,String cf) {
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
            tableDescriptor.addFamily(new HColumnDescriptor(cf));

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
    public static void insertData(String tableName,String cf,String rowkey,String col,String value) {
        System.out.println("start insert data ......");
        HTablePool pool = new HTablePool(configuration, 1000);
        Put put = new Put(rowkey.getBytes());// 一个PUT代表一行数据，再NEW一个PUT表示第二行数据,每行一个唯一的ROWKEY，此处rowkey为put构造方法中传入的值
        put.add(cf.getBytes(), col.getBytes(), value.getBytes());// 本行数据的第一列
        try {
            //如今应用的api版本中pool.getTable返回的类型是HTableInterface ，无法强转为HTable
            pool.getTable(tableName).put(put);
        }catch (Exception e){

        }
        System.out.println("end insert data ......");

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
    public static void QueryByCondition1(String tableName, String rowKet) {

        HTablePool pool = new HTablePool(configuration, 1000);
        try {
            Get scan = new Get(rowKet.getBytes());// 根据rowkey查询
            //如今应用的api版本中pool.getTable返回的类型是HTableInterface ，无法强转为HTable
            Result r = pool.getTable(tableName).get(scan);
            System.out.println("获得到rowkey:" + new String(r.getRow()));
            for (KeyValue keyValue : r.raw()) {
                System.out.println("列：" + new String(keyValue.getFamily())
                        + "====值:" + new String(keyValue.getValue()));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
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
                    System.out.println("列族： "+new String(keyValue.getFamily())+"==>,列：" + new String(keyValue.getKey())
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
