import java.sql.Date
import java.text.SimpleDateFormat
import java.util.Properties

import Kafka_Spark.json_an
import kafka.serializer.StringDecoder
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Duration, StreamingContext}
import org.apache.spark.streaming.kafka.KafkaUtils
import com.alibaba.fastjson.JSON
import kafka.producer.ProducerConfig
import kafka.producer.Producer
import kafka.producer.KeyedMessage
import org.apache.spark.{SparkConf, SparkContext}
//2013-03-15 12:39 – 74.125.26.121 /catalog/cat1.html 891ms - 2326
object log_deal {

  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("kafka-spark-demo")
    val scc = new StreamingContext(sparkConf, Duration(5000))

    //    scc.checkpoint(".") // 因为使用到了updateStateByKey,所以必须要设置checkpoint
    val topics = Set("kafka_spark3") //我们需要消费的kafka数据的topic
    val kafkaParam = Map(
      "metadata.broker.list" -> "localhost:9092", // kafka的broker list地址
      "auto.offset.reset" -> "smallest"
    )
    val stream: InputDStream[(String, String)] = createStream(scc, kafkaParam, topics)
    //对数据进行处理
    val wordCount = stream.map(l => (json_an(l._2).toString.split(" ")(4),json_an(l._2).toString.split(" ")(5).split("m")(0).toInt)).groupByKey().map{
      x=>(x._1, x._2.reduce(_+_)/x._2.count(x=>true))}.print()
    val wordCount2 = stream.map(l => (json_an(l._2).toString.split(" ")(4),1)).reduceByKey(_+_)

//    wordCount2.print()
    //发送数据(对外部服务器连接必须要用这种方式，不然会报错：任务无法序列化)
//    wordCount.foreachRDD { rdd =>
//      rdd.foreachPartition { partitionOfRecords =>
//        //配置说明
//        val producerProperties = new Properties()
//        producerProperties.put("serializer.class", "kafka.serializer.StringEncoder")
//        producerProperties.put("metadata.broker.list", "localhost:9092")
//        producerProperties.put("request.required.acks", "1")
//        val config: ProducerConfig = new ProducerConfig(producerProperties)
//        //与kafka进行连接。此处用的是kafka自家的Producer，用spark的kafkaproducer也可以，但传送的方式不同
//        val producer = new Producer[String,String](config)
//        partitionOfRecords.foreach(record =>
//          //发送数据，在这里key简单的用了相同的。实际情况应该用别的
//          producer.send(new KeyedMessage("cunchu","key",record.toString()))
//        )
//
//      }
//    }

    scc.start() // 真正启动程序
    scc.awaitTermination() //阻塞等待
  }
  /**
    * 创建一个从kafka获取数据的流.
    *
    * @param scc        spark streaming上下文
    * @param kafkaParam kafka相关配置
    * @param topics     需要消费的topic集合
    * @return
    */
  def createStream(scc: StreamingContext, kafkaParam: Map[String, String], topics: Set[String]) = {
    KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](scc, kafkaParam, topics)
  }

  def formatData(line: String) = {
    val date = new SimpleDateFormat("yyyy/MM/dd H:mm")
    val d = new SimpleDateFormat("yyyy/MM/dd")
    val dateFormated = date.parse(line)
    val dateFormated3 = date.parse(line.split(" ")(0) + " 0:0")

    val dateFormated2 = date.format(dateFormated)
    val dateFormated4 = date.format(dateFormated3)

    val dateFf = date.parse(dateFormated2).getTime
    val dateFf2 = date.parse(dateFormated4).getTime
    val r = dateFf - dateFf2
    val hash = r / 300000
    val final_date = new Date(hash.toInt * 300000 + dateFf2)
    date.format(final_date)
  }

  //字符串处理。在这里是提取时间
  def json_an(str: String) = {
    if (str.length < 10) {
      1
    }
    else {
      val json = JSON.parseObject(str)

      val main_v = json.get("payload")
      main_v
    }
  }
}