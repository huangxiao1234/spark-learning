package main.scala
import org.apache.spark.SparkContext._
import org.apache.spark.{SparkConf, SparkContext}
object log_spark_offline {
  def main(args: Array[String]) {
    if (args.length < 2) {
      println("Usage:SparkWordCount FileName")
      System.exit(1)
    }

    val conf = new SparkConf().setAppName("log_deal").setMaster("local")
    val sc = new SparkContext(conf)
    val textFile = sc.textFile(args(0))
    val avg = textFile.map(l =>(l.toString.split(" ")(4),l.toString.split(" ")(5).split("m")(0).toInt)).groupByKey().map{
      x=>(x._1, x._2.reduce(_+_)/x._2.count(x=>true))}
    avg.saveAsTextFile(args(1))
    sc.stop()
  }
}
