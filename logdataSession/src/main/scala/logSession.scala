import java.io.File
import java.sql.Date

import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

object logSession {

  //保存数据到数据库mysql
  private def insertMySQL(tableName: String, dataDF: DataFrame): Unit = {
    dataDF.write
      .format("jdbc")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("url", "jdbc:mysql://47.104.73.132:3306/logdata?useUnicode=true&characterEncoding=utf-8")
      .option("dbtable", tableName)
      .option("user", "wangshusen")
      .option("password", "wangshusen123")
      .mode(SaveMode.Overwrite)
      .save()
  }

  //从数据库mysql获取数据
  private def getDate(tableName: String, spark: SparkSession) = {
    spark.read
      .format("jdbc")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("url", "jdbc:mysql://47.104.73.132:3306/logdata?useUnicode=true&characterEncoding=utf-8")
      .option("dbtable", tableName)
      .option("user", "wangshusen")
      .option("password", "wangshusen123")
      .load()
  }

  //样例类
  case class actionopvidio(id: Int, uid: Int, rid: Int, rt: Int, at: Int, sp: Double, ct: String, ct2: String, cid: Int)

  def main(args: Array[String]) {

    //本地spark，测试使用
    val conf = new SparkConf().setMaster("local[*]").setAppName("logSession")

    //创建SparkContext，该对象是提交spark App的入口
    val sc = new SparkContext(conf)

    //创建sparksession
    val spark = SparkSession.builder().config(conf).getOrCreate()
    //导入隐式转换，支持sparksql的df
    import spark.implicits._
    //注册udf函数
    //    spark.udf.register("concat_long_string", (v1: Long, v2: Long, v3: Long, split: String) => v1.toString + split + v2 + split + v3)
    val jdbcDF = getDate("r_actionopvidio", spark)

    //创建临时表
    jdbcDF.createOrReplaceTempView("actionopvidio")

    //样例类
    case class keyMap(key: String, value: String)

    val tmptb = spark.sql("select id, uid,cid,rid,rt,ct,at,max(sp)over(partition by uid,cid,rid) spmax, row_number()over( order by uid ) row_num FROM actionopvidio")
    //创建临时表
    tmptb.createOrReplaceTempView("tmptb")

    val zstb = spark.sql("select uid,cid,rid,rt,ct,at,spmax,row_num,row_number()over(order by uid) num from tmptb where at=1 or row_num in(select row_num-1 from tmptb where at =1) ")
    //创建临时表
    zstb.createOrReplaceTempView("zstb")
    val mmtb = spark.sql("select uid,cid,rid,rt,at,lag(ct,1)over(partition by uid,cid,rid,rt order by ct) minct,ct maxct,spmax from zstb ")
    //    mmtb.show(200)
    //创建临时表
    mmtb.createOrReplaceTempView("mmtb")

    val nntb = spark.sql("select uid,cid,rid,rt,at,minct,maxct,spmax from mmtb where at !=1  ")
    //创建临时表
    nntb.createOrReplaceTempView("nntb")
    val tttb = spark.sql("select uid,cid,rid,rt,at,maxct minct,maxct,spmax from mmtb where at =1  ")
    //创建临时表
    tttb.createOrReplaceTempView("tttb")

    val mytb = spark.sql("select * from nntb union select * from tttb  ")
    //    mytb.show(200)
    //    存入数据库
    insertMySQL("s_longsession", mytb)

    //关闭连接
    spark.stop()
  }

}
