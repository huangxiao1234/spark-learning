import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.StreamingContext._

object test {
  def main(args: Array[String]) {
    //配置文件说明
    val sparkConf = new SparkConf().setAppName("FileWordCount").setMaster("local[2]")
//
    // 创建Streaming的上下文，包括Spark的配置和时间间隔，这里时间为间隔20秒
    val ssc = new StreamingContext(sparkConf, Seconds(20))

    // 指定监控的目录
    val lines = ssc.textFileStream("/Users/huangxiao/test_streaming/")

    // 对指定文件夹变化的数据进行单词统计并且打印
    val words = lines.flatMap(_.split(" "))
    val wordCounts = words.map(x => (x, 1)).reduceByKey(_ + _)
    wordCounts.print()

    // 启动Streaming
    ssc.start()
    ssc.awaitTermination()
  }
}