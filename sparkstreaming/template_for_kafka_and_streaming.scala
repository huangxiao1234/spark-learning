package main.scala

import kafka.api.{OffsetCommitRequest, OffsetFetchRequest, TopicMetadataRequest}
import kafka.common.{OffsetAndMetadata, TopicAndPartition}
import kafka.consumer.SimpleConsumer
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Duration, StreamingContext}
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils}


//需求：消费者自定义控制offset
//在这里offset保存到kafka内部的特殊topic：__consumer_offsets中，使用kafka.consumer.SimpleConsumer类来进行一系列操作
object template_for_kafka_and_streaming {


  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("kafka-spark-demo")
    val scc = new StreamingContext(sparkConf, Duration(5000)) //new一个spark-streaming的上下文

    val groupid = "user3"
    val topic = "kafka_test4"
    val kafkaParam = Map(
      "metadata.broker.list" -> "localhost:9092" // kafka的broker list地址
    )

    deal_with_streaing(scc, kafkaParam, topic, groupid)
    scc.start() // 真正启动程序
    scc.awaitTermination()
  }


  def setFromOffsets(list: List[(String, Int, Long)]): Map[TopicAndPartition, Long] = {
    var fromOffsets: Map[TopicAndPartition, Long] = Map()
    for (offset <- list) {
      val tp = TopicAndPartition(offset._1, offset._2) //topic和分区数
      fromOffsets += (tp -> offset._3) // offset位置
    }
    fromOffsets
  }

  def get_previous_offset(topic: String) = {
    val simpleConsumer = new SimpleConsumer("localhost", 9092, 1000000, 64 * 1024, "test")
    //new一个consumer并连接上kafka
    val topiclist = Seq(topic)
    val topicReq = new TopicMetadataRequest(topiclist, 0)
    //定义一个topic请求，为了获取相关topic的信息（不包括offset,有partition）
    val res = simpleConsumer.send(topicReq)
    //发送请求，得到kafka相应
    val topicMetaOption = res.topicsMetadata.headOption
    //定义一个Topicandpartition的格式，便于后面请求offset
    val topicAndPartition: Seq[TopicAndPartition] = topicMetaOption match {
      case Some(tm) => tm.partitionsMetadata.map(pm => TopicAndPartition(topic, pm.partitionId))
      case None => Seq[TopicAndPartition]()
    }
    val fetchRequest = OffsetFetchRequest("user3", topicAndPartition)
    //定义一个请求，传递的参数为groupid,topic,partitionid,这三个也正好能确定对应的offset的位置
    val fetchResponse = simpleConsumer.fetchOffsets(fetchRequest).requestInfo
    //向kafka发送请求并获取返回的offset信息
    val offsetl = fetchResponse.map { l =>
      val part_name = l._1.partition
      val offset_name = l._2.offset
      (topic, part_name, offset_name)
    }
    offsetl.toList
  }

  def create_streaming(scc: StreamingContext, offsetList: List[(String, Int, Long)], kafkaParam: Map[String, String]) = {
    val fromOffsets = setFromOffsets(offsetList)
    //对List进行处理，变成需要的格式，即Map[TopicAndPartition, Long]
    val messageHandler = (mam: MessageAndMetadata[String, String]) => (mam.topic, mam.message()) //构建MessageAndMetadata，这个所有使用情况都是一样的，就这么写
    //定义流.这种方法是不会在zookeeper的/consumers中创建一个新的groupid实例的
    val stream: InputDStream[(String, String)] = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, (String, String)](scc, kafkaParam, fromOffsets, messageHandler)
    stream
  }

  def deal_with_streaing(scc: StreamingContext, kafkaParam: Map[String, String], topic: String, groupid: String) = {
    val offsetlist = get_previous_offset(topic)
    val stream = create_streaming(scc, offsetlist, kafkaParam)
    stream.print() //对流的处理

    //将已更新的offset存入topic：__consumer_offsets中，以便下次使用
    //另外，这里涉及到与外部系统即kafka的连接，所以要使用一下结构
    stream.foreachRDD { rdd =>
      rdd.foreachPartition { partitionOfRecords =>
        //配置说明
        val simpleConsumer2 = new SimpleConsumer("localhost", 9092, 1000000, 64 * 1024, "test-client")
        partitionOfRecords.foreach { record =>
          val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges //这个语句可以返回当前rdd所更新到的offset值（OffsetRange(topic: 'kafka_test4', partition: 0, range: [1 -> 4])）
          for (o <- offsetRanges) {
            //在这里o.untilOffset返回的是offset末态
            //而o.fromOffset返回的是offset初态
            //所以看需求进行存储
            println(o)
            val topicAndPartition = TopicAndPartition(topic, o.partition)
            //定义一个格式
            val commitRequest = OffsetCommitRequest(groupid, Map(topicAndPartition -> OffsetAndMetadata(o.fromOffset)))
            //定义一个请求，注意，在这里存储的是fromOffset
            val commitResponse = simpleConsumer2.commitOffsets(commitRequest) //提交请求，完成offset存储即更新
          }
        }

      }
    }
  }

}

