package scala

import org.apache.spark.sql.{Row, SparkSession, types}
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}
import com.databricks.spark.avro._

//用spark完成对数据的提取和存储操作
//用Spark和Spark SQL分别实现以下功能：提取Eno，Ename，Esex，Dname，Daddr，用Avro格式将数据存到“/Quiz/名字/”目录下

object quiz1_for_spark_521 {
  def main(args: Array[String]) {
    if (args.length < 2) {
      println("Usage:SparkWordCount FileName")
      System.exit(1)
    }

    val conf = new SparkConf().setAppName("log_deal").setMaster("local")
    val sc = new SparkContext(conf)
    val sparkSession = SparkSession.builder().appName("RDD to DataFrame").config(conf).getOrCreate()//新的sql api，等价于sqlContext().


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
    //join两个RDD
    val joinRDD=EmpRDD.join(DeptRDD).map(x=>x._2._1+','+x._2._2)
      .map{l=>
      val l_l=l.split(',')
      val eno=l_l(0)
      val ename=l_l(1)
      val esex= l_l(2)
      val dname= l_l(3)
      val daddr= l_l(4)
      Row(eno,ename,esex,dname,daddr)}

    //在这里需要声明一下表的结构。因为arvo需要列名
    val schema_final = StructType(
      Seq(
        //这里的StringType注意别用错了，是org.apache.spark.sql.types.StringType。不注意的话会用到org.apache.spark.sql.StringType
        StructField("Eno",types.StringType,true)
        ,StructField("Ename",types.StringType,true)
        ,StructField("Esex",types.StringType,true)
        ,StructField("Dname",types.StringType,true)
        ,StructField("Daddr",types.StringType,true)
      )
    )
    //将RDD与表结构进行匹配，生成DF便于avro格式的存储
    val df=sparkSession.createDataFrame(joinRDD,schema_final)
    //这个方法是cloudera官网的实例，很多用法都有具体的例子 https://www.cloudera.com/documentation/enterprise/latest/topics/spark_avro.html
    df.write.format("com.databricks.spark.avro").save(args(2))
    sc.stop()
  }

}
