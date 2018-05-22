package scala

import org.apache.spark.{SparkConf, SparkContext}

object log_spark_offline {
  def main(args: Array[String]) {
    if (args.length < 2) {
      println("Usage:SparkWordCount FileName")
      System.exit(1)
    }

    val conf = new SparkConf().setAppName("log_deal")
    val sc = new SparkContext(conf)
    val textFile = sc.textFile(args(0))
    val avg = textFile.map(l =>deal_string(l))
      .reduceByKey((x,y)=>(x+y)/2)
    avg.saveAsTextFile(args(1))
    sc.stop()
  }
  def deal_string(line:String):(String,Int) ={
    val line_split=line.split(" ")
    if (line_split.length>=6) {
      val content = line_split(4)
      val time = line_split(5).split("m")(0)
      if (content.startsWith("/") && time.length != 0) {
        (content, time.toInt)
      }
      else {
        ("NAN", 1)
      }
    }
    else {
      ("NAN", 1)
    }
  }
}
