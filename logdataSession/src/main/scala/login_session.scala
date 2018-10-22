import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}


object login_session {


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
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("logSession")

    //创建SparkContext，该对象是提交spark App的入口
    val sc = new SparkContext(sparkConf)

    //创建sparksession
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    //登陆会话的数据
    val jdbcDF = getDate("r_loginop", spark)
    val jdbcDF2 = getDate("r_timestat", spark)
    jdbcDF2.createOrReplaceTempView("r_timestat")
    //创建临时表
    jdbcDF.createOrReplaceTempView("r_loginop")
    val unalltb = spark.sql("select id,uid,appid,lo,la,net,de,v,rt,at,sn,ct FROM r_loginop union  select 0,uid,0,0,0,0,0,0,0,0,0,ct from r_timestat order by uid")
    unalltb.createOrReplaceTempView("unalltb")
    //查询所有数据并按uid.cid,rid,rt排序新添加一列row_num作为唯一标识
    //    val tmptb = spark.sql("select id, uid,cid,rid,rt,ct,at,max(sp)over(partition by uid,cid,rid) spmax, row_number()over( order by uid,cid,rid,ct ) row_num FROM actionopvidio")
    val tmptb = spark.sql("select id,uid,appid,lo,la,net,de,v,rt,at,sn,ct, row_number()over( order by uid,rt,ct ) row_num FROM unalltb")
    //创建临时表
    tmptb.createOrReplaceTempView("tmptb")
    //    tmptb.show(100)
    //获取at=1和at=1的前一条数据，即为获取一个uid，cid,rid,rt的登陆到离开的会话
    //    val zstb = spark.sql("select id,uid,cid,rid,rt,ct,at,spmax,row_num,row_number()over(order by uid,cid,rid,ct) num from tmptb where at=1 or row_num in(select row_num-1 from tmptb where at =1) ")
    val zstb = spark.sql("select * from tmptb where at=1 or row_num in(select row_num-1 from tmptb where at =1) ")
    //创建临时表
    zstb.createOrReplaceTempView("zstb")
    //        zstb.show(100)

    //再添加一列tag为row_num的lag（）列
    val nntb = spark.sql("select id,uid,appid,lo,la,net,de,v,rt,at,sn,ct,row_num,lag(row_num,1) over( order by row_num) tag from zstb  order by uid,rt,ct")
    //        nntb.show(200)
    //创建临时表
    nntb.createOrReplaceTempView("nntb")

    //和并at==1和at!=1的俩张表，并tag一个会话时指相同（正好一个会话里面只有俩条数据和lag满足）
    val mmtb = spark.sql("select id,uid,appid,lo,la,net,de,v,rt,at,sn,ct,tag from nntb where at !=1 union all select id,uid,appid,lo,la,net,de,v,rt,at,sn,ct,row_num tag from nntb where at ==1 order by uid,rt,ct")
    mmtb.show(200)
    //创建临时表
    mmtb.createOrReplaceTempView("mmtb")

    //然后就可以成为uid，cid，rid，rt，tag为一个分组，去ct的最大值和最小值就是离开和进入的时间
    val mytb = spark.sql("select min(id) maxindex,uid,appid,lo,la,net,de,v,rt,sn,tag,min(ct) minct,max(ct) maxct from mmtb group by uid,appid,lo,la,net,de,v,rt,sn,tag order by uid,rt,tag")
    mytb.show(200)
    mytb.createOrReplaceTempView("mytb")
    val dbtable = spark.sql("select maxindex,uid,rt,appid,lo,la,net,de,v,sn,minct,maxct from mytb")
    //创建临时表

    //    存入数据库
    insertMySQL("session_login", dbtable)

    //使用spark的内置方法，可以实现分区，抓取等功能


    //关闭连接
    spark.stop()
  }

}
