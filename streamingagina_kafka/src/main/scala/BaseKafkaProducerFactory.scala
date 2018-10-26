import java.util.Properties

import org.apache.commons.pool2.{BasePooledObjectFactory, PooledObject}
import org.apache.commons.pool2.impl.DefaultPooledObject

/**
  * Created by wshs on 09/10/2018.
  */
abstract class KafkaProducerFactory(brokerList: String, config: Properties, topic: Option[String] = None) extends Serializable {

  def newInstance(): KafkaProducerProxy
}
  class BaseKafkaProducerFactory(brokerList: String,
                                 config: Properties = new Properties,
                                 defaultTopic: Option[String] = None)
    extends KafkaProducerFactory(brokerList, config, defaultTopic) {

    override def newInstance() = new KafkaProducerProxy(brokerList, config, defaultTopic)

  }


  class PooledKafkaProducerAppFactory(val factory: KafkaProducerFactory)
    extends BasePooledObjectFactory[KafkaProducerProxy] with Serializable {

    override def create(): KafkaProducerProxy = factory.newInstance()

    override def wrap(obj: KafkaProducerProxy): PooledObject[KafkaProducerProxy] = new DefaultPooledObject(obj)

    override def destroyObject(p: PooledObject[KafkaProducerProxy]): Unit = {
      p.getObject.shutdown()
      super.destroyObject(p)
    }

}
