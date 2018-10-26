import conf.ConfigurationManager
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object TestKafkaStreaming {


  def main(args: Array[String]): Unit = {

    // 构建Spark上下文
    val sparkConf = new SparkConf().setAppName("streamingRecommendingSystem").setMaster("local[*]")

    // 创建Spark客户端
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    val sc = spark.sparkContext
    val ssc = new StreamingContext(sc, Seconds(20))

    // 设置检查点目录
    ssc.checkpoint("./streaming_checkpoint")

    // 获取Kafka配置
    val broker_list = ConfigurationManager.config.getString("kafka.broker.list")
    val topics = ConfigurationManager.config.getString("kafka.topics")

    // kafka消费者配置
    val kafkaParam = Map(
      "bootstrap.servers" -> broker_list, //用于初始化链接到集群的地址
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      //用于标识这个消费者属于哪个消费团体
      "group.id" -> "commerce-consumer-group",
      //如果没有初始化偏移量或者当前的偏移量不存在任何服务器上，可以使用这个配置属性
      //可以使用这个配置，latest自动重置偏移量为最新的偏移量
      "auto.offset.reset" -> "latest",
      //如果是true，则这个消费者的偏移量会在后台自动提交
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    // 创建DStream，返回接收到的输入数据
    // LocationStrategies：根据给定的主题和集群地址创建consumer
    // LocationStrategies.PreferConsistent：持续的在所有Executor之间分配分区
    // ConsumerStrategies：选择如何在Driver和Executor上创建和配置Kafka Consumer
    // ConsumerStrategies.Subscribe：订阅一系列主题
    val adRealTimeLogDStream = KafkaUtils.createDirectStream[String, String](ssc, LocationStrategies.PreferConsistent, ConsumerStrategies.Subscribe[String, String](Array(topics), kafkaParam))
//    adRealTimeLogDStream.print()

   /* //每一个stream都是一个ConsumerRecord
    stream.map(s =>("id:" + s.key(),">>>>:"+s.value())).foreachRDD(rdd => {
      rdd.foreachPartition(partitionOfRecords => {
        // Get a producer from the shared pool
        val pool = createKafkaProducerPool(brobrokers, targettopic)
        val p = pool.borrowObject()

        partitionOfRecords.foreach {message => System.out.println(message._2);p.send(message._2,Option(targettopic))}

        // Returning the producer to the pool also shuts it down
        pool.returnObject(p)

      })
    })*/

    var adRealTimeValueDStream = adRealTimeLogDStream.map(consumerRecordRDD => consumerRecordRDD.value())

    // 用于Kafka Stream的线程非安全问题，重新分区切断血统
//    adRealTimeValueDStream = adRealTimeValueDStream.repartition(400)
    adRealTimeValueDStream.foreachRDD(rdd => {
//        rdd.collect()
//      rdd.foreachPartition(rddpar=>
//        rddpar.foreach(aa=>
//
//        )
//          rddpar.foreach(rdds=>
//               rdds.length
//
//          )

//      )




        }
    )

//    adRealTimeValueDStream.count()
    /* val rowsDStream = adRealTimeValueDStream.transform { consumerRecordRDD =>

       val lineString = consumerRecordRDD.map { lines =>
         val line = lines.split(",")

         (line(0), line(1), line(2), line(3), line(4), line(5))

       }
       lineString
     }
     rowsDStream.foreachRDD( rdd=>
       rdd.collect()

     )*/

    //    adRealTimeValueDStream.print()

    /*// 根据动态黑名单进行数据过滤 (userid, timestamp province city userid adid)
    val filteredAdRealTimeLogDStream = filterByBlacklist(spark,adRealTimeValueDStream)

    // 业务功能一：生成动态黑名单
    generateDynamicBlacklist(filteredAdRealTimeLogDStream)

    // 业务功能二：计算广告点击流量实时统计结果（yyyyMMdd_province_city_adid,clickCount）
    val adRealTimeStatDStream = calculateRealTimeStat(filteredAdRealTimeLogDStream)

    // 业务功能三：实时统计每天每个省份top3热门广告
    calculateProvinceTop3Ad(spark,adRealTimeStatDStream)

    // 业务功能四：实时统计每天每个广告在最近1小时的滑动窗口内的点击趋势（每分钟的点击量）
    calculateAdClickCountByWindow(adRealTimeValueDStream)*/

    ssc.start()
    ssc.awaitTermination()
  }

}
