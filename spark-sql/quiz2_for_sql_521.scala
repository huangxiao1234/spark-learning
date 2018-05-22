package scala

import org.apache.spark.sql.{Row, SparkSession, types}
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}
import com.databricks.spark.avro._

//对上一提结果进行统计，Esex=‘M’，对Dname进行统计，将结果输出到xx目录下，存储格式用Parquet，用‘\t’分隔

object quiz2_for_sql_521 {
  def main(args: Array[String]) {
    if (args.length < 2) {
      println("Usage:SparkWordCount FileName")
      System.exit(1)
    }

    val conf = new SparkConf().setAppName("log_deal").setMaster("local")
    val sc = new SparkContext(conf)
    val sparkSession = SparkSession.builder().appName("RDD to DataFrame")
      .config(conf).getOrCreate()

    val EmpFile = sc.textFile(args(0))
    val EmpRDD = EmpFile.map{l=>
      val l_l=l.split(',')
      val eno=l_l(0)
      val ename=l_l(1)
      val esex= l_l(2)
      val edno = l_l(3)
      //注意这个Row很关键，只有将类型变成了Row后面才能完成RDD转换成DataFrame的操作
      Row(eno,ename,esex,edno)}

    val schema_E = StructType(
      Seq(
        StructField("Eno",types.StringType,true)
        ,StructField("Ename",types.StringType,true)
        ,StructField("Esex",types.StringType,true)
        ,StructField("Dno",types.StringType,true)
      )
    )
    val df_E=sparkSession.createDataFrame(EmpRDD,schema_E)
    //    df_E.createTempView("E")

    val DeptFile = sc.textFile(args(1))
    val DeptRDD = DeptFile.map{l=>
      val l_l=l.split(',')
      val dno=l_l(0)
      val dname=l_l(1)
      val daddr= l_l(2)
      Row(dno,dname,daddr)}

    val schema = StructType(
      Seq(
        StructField("Dno",types.StringType,true)
        ,StructField("Dname",types.StringType,true)
        ,StructField("Daddr",types.StringType,true)
      )
    )
    val df_D=sparkSession.createDataFrame(DeptRDD,schema)
    //    df_D.createTempView("D")
    val final_df=df_E.join(df_D,"Dno")
    final_df.createTempView("E_D")
    //直接使用SQL语句就能操作
    val results = sparkSession.sql("SELECT Dname,count(Esex) as Number FROM E_D where Esex='M' group by Dname")
    results.show()
    results.write.format("parquet").option("delimiter", "\t").save(args(2))
    sc.stop()
  }

}
