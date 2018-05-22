package scala

import org.apache.spark.{SparkConf, SparkContext}

object spark_sql {
  def main(args: Array[String]) {
    if (args.length < 1) {
      println("Usage:SparkWordCount FileName")
      System.exit(1)
    }


    val conf = new SparkConf().setAppName("log_deal1").setMaster("local")
    val sc = new SparkContext(conf)
    val textFile = sc.textFile(args(0))
    val a = textFile.filter(l=>l.contains("bee")).map(line => choose(line)).foreach(println)
//        a.saveAsTextFile(args(1))
    sc.stop()
  }
  def choose(str: String) = {
          val line=str.split(",")
          val s=str
          val id =line(0)
          val animal = line(1)
          if (animal=="bee"){
          id
          }
  }


}
