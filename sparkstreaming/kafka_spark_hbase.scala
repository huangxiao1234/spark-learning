package main.scala
//需求：
//1.hbase中有数据 （用户id，城市id）000001 shanghai
//2.kafka中不停地生产用户操作日志  （用户id,操作，时间） 000001 w 2018-5-17 09:00
//3.需要每5分钟统计一次操作日志的用户的数量，按城市划分
//kafka topic: city_count

import java.sql.Date
import java.text.SimpleDateFormat
import java.util.Properties

import kafka.serializer.StringDecoder
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Duration, StreamingContext}
import org.apache.spark.streaming.kafka.KafkaUtils
import com.alibaba.fastjson.JSON
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
//import org.apache.hadoop.hbase.client.{ConnectionFactory, Get}
import org.apache.hadoop.hbase.client.{ConnectionFactory, Get}
//import kafka.producer.ProducerConfig
//import kafka.producer.Producer
//import kafka.producer.KeyedMessage
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.hadoop.hbase.{HBaseConfiguration, HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.hadoop.hbase.client
import org.apache.hadoop.hbase.util.Bytes

//1.打开zk,kafka。2.启动kafka-connect(source部分)3.运行此文件
object kafka_spark_hbase {
  val configuration = HBaseConfiguration.create()
  configuration.set("hbase.zookeeper.property.clientPort", "2181")
  configuration.set("hbase.zookeeper.quorum", "localhost")
  val connection = ConnectionFactory.createConnection(configuration)
  val admin = connection.getAdmin()
  val table = connection.getTable(TableName.valueOf("user_city"))
  val table2 = connection.getTable(TableName.valueOf("time_city"))
  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("kafka-spark-demo")
    val scc = new org.apache.spark.streaming.StreamingContext(sparkConf, Duration(10000))//new一个spark-streaming的上下文

    //    scc.checkpoint(".") // 暂时用不到
    val topics = Set("city_count") //我们需要消费的kafka数据的topic
    val kafkaParam = Map(
      "metadata.broker.list" -> "localhost:9092", // kafka的broker list地址
      "auto.offset.reset" -> "smallest"//这个参数可以让streaming消费topic的时候从头开始消费
    )
    val stream: InputDStream[(String, String)] = createStream(scc, kafkaParam, topics)//建立流，读数据，传入上下文，kafka配置，主题名字
//    val wordCount = stream.map(l => (json_an(l._2), 1)).reduceByKey(_ + _) //对数据进行处理
    val wordCount = stream.map(l=>json_an(l._2)).map(l=>(deal_user_city(l.toString),1)).reduceByKey(_+_)
    wordCount.map{line=>
    //准备插入一条 key 为 id001 的数据
    val time=line._1.toString
    val city=line._2.toString
    val p = new Put(time.getBytes)
    //为put操作指定 column 和 value （以前的 put.add 方法被弃用了）
    p.addColumn("info".getBytes, "city".getBytes, city.getBytes)
    //提交
    table2.put(p)
      "store successs!"
  }.print()
//    wordCount.print()//输出到控制台看看结果


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
    val date = new SimpleDateFormat("yyyy-MM-dd H:mm")
    val d = new SimpleDateFormat("yyyy-MM-dd")
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
//
  def deal_user_city(str:String)={
    //(time,id)
    val str_1=str.split(',')
    val id=str_1(1).split(')')(0)
    val g = new Get(id.getBytes)
    val result = table.get(g)
    val value = Bytes.toString(result.getValue("info".getBytes,"city".getBytes))
    str_1(0).split('(')(1)+' '+value

  }
  //字符串处理。在这里是提取时间
  def json_an(str: String) = {
    if (str.length < 10) {
      1
    }
    else {
      val json = JSON.parseObject(str)

      val main_v = json.get("payload")
      val v= main_v.toString.split(",")
      if (v.length == 3) {
        (formatData(v(2)),v(0))
      }
      else {
        "NAN"
      }
    }
  }
  def convert(triple: (String, Int)) = {
    val p = new Put(Bytes.toBytes(triple._1))
    p.addColumn(Bytes.toBytes("info"),Bytes.toBytes("city"),Bytes.toBytes(triple._2))
    (new ImmutableBytesWritable, p)
  }
}
