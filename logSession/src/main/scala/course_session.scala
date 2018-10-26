

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

object course_session {

  //保存数据到数据库mysql
  private def insertMySQL(tableName: String, dataDF: DataFrame): Unit = {
    dataDF.write
      .format("jdbc")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("url", "jdbc:mysql://47.104.73.132:3306/statdb?useUnicode=true&characterEncoding=utf-8")
      .option("dbtable", tableName)
      .option("user", "wangshusen")
      .option("password", "wangshusen123")
      .mode(SaveMode.Append)
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

    val jdbcDF = getDate("r_courseop", spark)
    val jdbcDF2 = getDate("r_timestat", spark)
    jdbcDF.createOrReplaceTempView("r_courseop")
    jdbcDF2.createOrReplaceTempView("r_timestat")

    val unalltb = spark.sql("select id,uid,cid,appid,sn,at,ct FROM r_courseop union  select 0,uid,cid,0,0,0,ct from r_timestat order by uid,cid")
    unalltb.createOrReplaceTempView("unalltb")

    //查询所有数据并按uid.cid,rid,rt排序新添加一列row_num作为唯一标识
    val tmptb = spark.sql("select id,uid,cid,appid,sn,at,ct, row_number()over( order by uid,cid,ct ) row_num FROM unalltb")
    //创建临时表
    tmptb.createOrReplaceTempView("tmptb")
    //获取at=1和at=1的前一条数据，即为获取一个uid，cid,rid,rt的登陆到离开的会话
    val zstb = spark.sql("select id,uid,cid,appid,sn,at,ct,row_num from tmptb where at=1 or row_num in(select row_num-1 from tmptb where at =1) ")
    //创建临时表
    zstb.createOrReplaceTempView("zstb")

    //再添加一列tag为row_num的lag（）列
    val nntb = spark.sql("select id,uid,cid,appid,sn,at,ct,row_num,lag(row_num,1) over( order by row_num) tag from zstb  order by uid,cid,ct")
    //创建临时表
    nntb.createOrReplaceTempView("nntb")

    //和并at==1和at!=1的俩张表，并tag一个会话时指相同（正好一个会话里面只有俩条数据和lag满足）
    val mmtb = spark.sql("select id,uid,cid,appid,sn,at,ct,tag from nntb where at !=1 union all select id,uid,cid,appid,sn,at,ct,row_num tag from nntb where at ==1 order by uid,cid,ct")
    mmtb.createOrReplaceTempView("mmtb")
    val mytb = spark.sql("select * ,row_number()over(order by uid,cid,tag)  row_num from mmtb  order by uid,cid,tag")
    mytb.createOrReplaceTempView("mytb")
    val testdb = spark.sql("select min(row_num) row_num,min(ct) minct,max(ct) maxct from mytb group by uid,cid,tag order by row_num ")
    testdb.createOrReplaceTempView("mytb1")
    val mytb2 = spark.sql("" +
      "select t2.id maxindex,t2.uid uid,t2.cid cid,t2.appid appid,t2.sn sn,t1.minct minct,t1.maxct maxct from " +
      "mytb1 t1 inner join mytb t2 on (t1.row_num == t2.row_num) ")
//    mytb2.show(200)
    //    存入数据库
    insertMySQL("session_course", mytb2)
    //关闭连接
    spark.stop()
  }


}
