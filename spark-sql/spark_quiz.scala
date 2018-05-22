package scala

import org.apache.spark.sql.types._
import org.apache.spark._
import org.apache.spark.sql._
//实现parquet格式的存储
object spark_quiz {
  def main(args: Array[String]) {
    if (args.length < 1) {
      println("Usage:SparkWordCount FileName")
      System.exit(1)
    }


    val conf = new SparkConf().setAppName("log_deal1").setMaster("local")
    val sc = new SparkContext(conf)

    val sparkSession = SparkSession.builder().appName("RDD to DataFrame").config(conf).getOrCreate()
    val textFile = sc.textFile(args(0))
    val a = textFile.map{l=>
            val l_l=l.split('|')
            val first_name=l_l(0)
            val last_name=l_l(1)
      (last_name+' '+first_name,l_l(2))}.sortByKey().map( x => Row(x._1,x._2.toInt)).foreach(println)
    val schema = StructType(
      Seq(
        StructField("name",types.StringType,true)
        ,StructField("age",IntegerType,true)
        )
    )
//    val df=sparkSession.createDataFrame(a,schema)
//    df.write.format("parquet").save("hdfs://localhost:9000/final.parquet")

    sc.stop()
  }
}
