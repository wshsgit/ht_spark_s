import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object course_activity_session {

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
    //导入隐式转换，支持sparksql的df
    //注册udf函数
    //    spark.udf.register("concat_long_string", (v1: Long, v2: Long, v3: Long, split: String) => v1.toString + split + v2 + split + v3)
    val jdbcDF = getDate("r_activityop", spark)
    val jdbcDF2 = getDate("r_timestat", spark)
    jdbcDF2.createOrReplaceTempView("r_timestat")

    //创建临时表
    jdbcDF.createOrReplaceTempView("r_activityop")

    //样例类
    case class keyMap(key: String, value: String)

    //查询所有数据并按uid.cid,rid,rt排序新添加一列row_num作为唯一标识
    val unalltb = spark.sql("select id,uid,rid,aid,rt,ct,at FROM r_activityop union  select 0,uid, rid,0,rt, ct,0 from r_timestat order by uid,rid")
    unalltb.createOrReplaceTempView("unalltb")
//    unalltb.show(200)
//    val jointb=spark.sql("select r_activityop.id,unalltb.uid,unalltb.rid,r_activityop.aid,r_activityop.rt,unalltb.ct,r_activityop.at from r_activityop left jon unalltb on r_activityop.uid=unalltb.uid and r_activityop.rid=unalltb.rid")
//    jointb.show(200)
    val tmptb = spark.sql("select id,uid,rid,aid,rt,ct,at, row_number()over( order by uid,rid,ct ) row_num FROM unalltb")
//    //创建临时表
    tmptb.createOrReplaceTempView("tmptb")
//    tmptb.show(100)
//    //获取at=1和at=1的前一条数据，即为获取一个uid，cid,rid,rt的登陆到离开的会话
    val zstb = spark.sql("select id,uid,rid,aid,rt,ct,at,row_num from tmptb where at=1 or row_num in(select row_num-1 from tmptb where at =1) ")
//    //创建临时表
    zstb.createOrReplaceTempView("zstb")
//    zstb.show(100)
//
//    //再添加一列tag为row_num的lag（）列
    val nntb = spark.sql("select id,uid,rid,aid,rt,ct,at,row_num,lag(row_num,1) over( order by row_num) tag from zstb  order by uid,rid,rt,ct")
//    nntb.show(20)
//    //创建临时表
    nntb.createOrReplaceTempView("nntb")
//
//    //和并at==1和at!=1的俩张表，并tag一个会话时指相同（正好一个会话里面只有俩条数据和lag满足）
    val mmtb = spark.sql("select id,uid,rid,aid,rt,ct,at,tag from nntb where at !=1 union all select id,uid,rid,aid,rt,ct,at,row_num tag from nntb where at ==1 order by uid,rid,rt,ct")
//    mmtb.show(200)
//    //创建临时表
    mmtb.createOrReplaceTempView("mmtb")
//
//    //然后就可以成为uid，cid，rid，rt，tag为一个分组，去ct的最大值和最小值就是离开和进入的时间
    val mytb = spark.sql("select min(id) maxindex,uid,rid,max(aid) maxaid,rt,tag,min(ct) minct,max(ct) maxct from mmtb group by uid,rid,rt,tag order by uid,rid,rt")
//    mytb.show(200)
//    //创建临时表
    mytb.createOrReplaceTempView("mytb")
    val dbtb=spark.sql("select maxindex,uid,rid,maxaid,rt,minct,maxct from mytb order by uid,rid")
//    dbtb.show(200)
//    //    存入数据库
    insertMySQL("session_course_activity", dbtb)

    //使用spark的内置方法，可以实现分区，抓取等功能
    //关闭连接
    spark.stop()
  }

}
