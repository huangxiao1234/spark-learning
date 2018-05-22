import java.sql.Date
import java.text.SimpleDateFormat
import java.util.Properties
import kafka.serializer.StringDecoder
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Duration, StreamingContext}
import org.apache.spark.streaming.kafka.KafkaUtils
import com.alibaba.fastjson.JSON
import kafka.producer.ProducerConfig
import kafka.producer.Producer
import kafka.producer.KeyedMessage
import org.apache.spark.{SparkConf, SparkContext}
//1.打开zk,kafka。2.启动kafka-connect(source部分)3.运行此文件
object Kafka_Spark {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("kafka-spark-demo")
    val scc = new StreamingContext(sparkConf, Duration(5000))//new一个spark-streaming的上下文

    //    scc.checkpoint(".") // 暂时用不到
    val topics = Set("kafka_for_example") //我们需要消费的kafka数据的topic
    val kafkaParam = Map(
      "metadata.broker.list" -> "localhost:9092", // kafka的broker list地址
      "auto.offset.reset" -> "smallest"//这个参数可以让streaming消费topic的时候从头开始消费
    )
    val stream: InputDStream[(String, String)] = createStream(scc, kafkaParam, topics)//建立流，读数据，传入上下文，kafka配置，主题名字
    val wordCount = stream.map(l => (json_an(l._2), 1)).reduceByKey(_ + _) //对数据进行处理
    wordCount.print()//输出到控制台看看结果

    //发送数据(对外部服务器连接必须要用这种方式，不然会报错：任务无法序列化)
    wordCount.foreachRDD { rdd =>
      rdd.foreachPartition { partitionOfRecords =>
        //配置说明
        val producerProperties = new Properties()
        producerProperties.put("serializer.class", "kafka.serializer.StringEncoder")
        producerProperties.put("metadata.broker.list", "localhost:9092")
        producerProperties.put("request.required.acks", "1")
        val config: ProducerConfig = new ProducerConfig(producerProperties)
        //与kafka进行连接。此处用的是kafka自家的Producer，用spark的kafkaproducer也可以，但传送的方式不同
        val producer = new Producer[String,String](config)
        partitionOfRecords.foreach(record =>
          //发送数据，在这里key简单的用了相同的。实际情况应该用别的
          producer.send(new KeyedMessage("cunchu","key",record.toString()))
        )

      }
    }

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
  //处理时间
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
      if (main_v.toString.split(",").length == 7) {
        formatData(main_v.toString.split(",")(2))
      }
      else {
        "NAN"
      }
    }
  }
}