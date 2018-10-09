
  import org.apache.commons.pool2.impl.{GenericObjectPool, GenericObjectPoolConfig}

  import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}


  /**
    * Created by wshs on 09/10/2018.
    */
  object createKafkaProducerPool{

    def apply(brokerList: String, topic: String):  GenericObjectPool[KafkaProducerProxy] = {
      val producerFactory = new BaseKafkaProducerFactory(brokerList, defaultTopic = Option(topic))
      val pooledProducerFactory = new PooledKafkaProducerAppFactory(producerFactory)
      val poolConfig = {
        val c = new GenericObjectPoolConfig
        val maxNumProducers = 10
        c.setMaxTotal(maxNumProducers)
        c.setMaxIdle(maxNumProducers)
        c
      }
      new GenericObjectPool[KafkaProducerProxy](pooledProducerFactory, poolConfig)
    }
  }

