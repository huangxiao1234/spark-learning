package scala

import org.apache.avro.Schema
import org.apache.avro.mapred.AvroKey
import org.apache.avro.mapreduce.{AvroJob, AvroKeyOutputFormat}
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.rdd.{PairRDDFunctions, RDD}
import org.apache.spark.sql.{Row, SparkSession, types}
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}

//用spark完成对数据的提取和存储操作
//用Spark和Spark SQL分别实现以下功能：提取Eno，Ename，Esex，Dname，Daddr，用Avro格式将数据存到“/Quiz/名字/”目录下

object quiz2_for_spark_521 {
  def main(args: Array[String]) {
    if (args.length < 2) {
      println("Usage:SparkWordCount FileName")
      System.exit(1)
    }

    val conf = new SparkConf().setAppName("log_deal").setMaster("local")
    val sc = new SparkContext(conf)
    val sparkSession = SparkSession.builder().appName("RDD to DataFrame").config(conf).getOrCreate()

    val Deptfile = sc.textFile(args(1))
    val DeptRDD=Deptfile.map{l =>
      val line = l.split(',')
      val dno=line(0)
      val dname=line(1)
      val daddr=line(2)
      (dno,dname+','+daddr)
    }

    val Empfile = sc.textFile(args(0))
    val EmpRDD=Empfile.map{l =>
      val line=l.split(',')
      val name=line(1)
      val sex=line(2)
      val edno=line(3)
      (edno,line(0)+','+name+','+sex)}


    //join两个表即可得结果，使用map语句进行数据的筛选，使用reduce统计
    val joinRDD=EmpRDD.join(DeptRDD).map(x=>x._2._1+','+x._2._2).filter(l=>l.contains("M")).map { x =>
      val line = x.split(",")
      val dname = line(3)
      (dname, 1)
    }.reduceByKey(_+_).map( x => Row(x._1,x._2.toInt))
    joinRDD.foreach(println)
    val schema = StructType(
      Seq(
        StructField("name",types.StringType,true)
        ,StructField("count",IntegerType,true)
      )
    )
        val df=sparkSession.createDataFrame(joinRDD,schema)
        df.write.format("parquet").option("delimiter", "\t").save(args(2))

    sc.stop()
  }

}
