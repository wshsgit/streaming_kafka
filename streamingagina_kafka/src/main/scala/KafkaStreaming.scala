
import kafka.api.{OffsetRequest, PartitionOffsetRequestInfo, TopicMetadataRequest}
import kafka.common.TopicAndPartition
import kafka.consumer.SimpleConsumer
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import kafka.utils.{ZKGroupTopicDirs, ZkUtils}
import org.I0Itec.zkclient.ZkClient
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by wshs on 09/10/2018.
  */
//object createKafkaProducerPool{
//
//  def apply(brokerList: String, topic: String):  GenericObjectPool[KafkaProducerProxy] = {
//    val producerFactory = new BaseKafkaProducerFactory(brokerList, defaultTopic = Option(topic))
//    val pooledProducerFactory = new PooledKafkaProducerAppFactory(producerFactory)
//    val poolConfig = {
//      val c = new GenericObjectPoolConfig
//      val maxNumProducers = 10
//      c.setMaxTotal(maxNumProducers)
//      c.setMaxIdle(maxNumProducers)
//      c
//    }
//    new GenericObjectPool[KafkaProducerProxy](pooledProducerFactory, poolConfig)
//  }
//}

object KafkaStreaming {

  def main(args: Array[String]) {

    val conf = new SparkConf().setMaster("local[4]").setAppName("NetworkTaoZhi")
    val ssc = new StreamingContext(conf, Seconds(5))

    //创建topic
    val brobrokers = "master01:9092,slave02:9092,slave03:9092"
    val zookeeper = "master01:2181,slave01:2181,slave02:2181"
    val sourcetopic = "tzuser_paper_summary";
    val groupid = "consumer101"

    //创建消费者组
    var group = "con-consumer-group"
    //消费者配置
    val kafkaParam = Map(
      "bootstrap.servers" -> brobrokers, //用于初始化链接到集群的地址
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      //用于标识这个消费者属于哪个消费团体
      "group.id" -> group,
      //如果没有初始化偏移量或者当前的偏移量不存在任何服务器上，可以使用这个配置属性
      //可以使用这个配置，latest自动重置偏移量为最新的偏移量
      "auto.offset.reset" -> "latest",
      //如果是true，则这个消费者的偏移量会在后台自动提交
      "enable.auto.commit" -> (false: java.lang.Boolean)
    );


//    KafkaUtils.createStream()

    //ssc.sparkContext.broadcast(pool)

    //创建DStream，返回接收到的输入数据
    var textKafkaDStream = KafkaUtils.createDirectStream[String, String](ssc, LocationStrategies.PreferConsistent, ConsumerStrategies.Subscribe[String, String](Array(sourcetopic), kafkaParam))
    val streamRdd = textKafkaDStream.map(consumerRecordRDD => consumerRecordRDD.value())
    streamRdd.print()
    var offsetRanges = Array[OffsetRange]()
    //注意，要想获得offsetRanges必须作为第一步

    val textKafkaDStream2 = textKafkaDStream.transform{ rdd =>
      offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd
    }

    textKafkaDStream2.map(s => "key:"+s._1 + " value:"+s._2).foreachRDD{rdd =>

//      //RDD操作
//      rdd.foreachPartition{items =>
//        //需要用到连接池技术
//        //创建到Kafka的连接
//        val pool = KafkaConnPool(brokenList)
//        //拿到连接
//        val kafkaProxy = pool.borrowObject()
//
//        //插入数据
//        for(item <- items)
//          kafkaProxy.send(targetTopic, item)
//
//        //返回连接
//        pool.returnObject(kafkaProxy)
//      }

      //保存Offset到ZK
      val updateTopicDirs = new ZKGroupTopicDirs(groupid, sourcetopic)
      val updateZkClient = new ZkClient(zookeeper)
      for(offset <- offsetRanges){
        //将更新写入到Path
        println(offset)
        val zkPath = s"${updateTopicDirs.consumerOffsetDir}/${offset.partition}"
        ZkUtils.updatePersistentPath(updateZkClient, zkPath, offset.fromOffset.toString)
      }
      updateZkClient.close()

    }


    ssc.start()
    ssc.awaitTermination()

  }


}

