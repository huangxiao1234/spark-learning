import java.sql.Date
import java.text.SimpleDateFormat

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Milliseconds, Seconds, StreamingContext}
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.storage.StorageLevel
//ouMrq2r_aU1mtKRTmQclGo1UzY 3251210381 2018/5/4 19:04 上海 上海 210.2.2.6 7038004
object LogAnalyse {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("LogAnalyse").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(10))

    val lines = ssc.socketTextStream(args(0), args(1).toInt, StorageLevel.MEMORY_AND_DISK_SER)

    //处理日期，使其5分钟之内的归为同一个日期
    def formatData(line:String)= {
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
    // 对读入的数据进行分割、计数
    val words = lines.map(line => (formatData(line.split(",")(2)),1))
    val wordCounts = words.reduceByKey(_ + _)

    wordCounts.print()
    ssc.start()
    ssc.awaitTermination()
  }
}