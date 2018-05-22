package main.scala
import java.io.IOException

import org.apache.hadoop.hbase.{HBaseConfiguration, HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.hadoop.hbase.client
import org.apache.hadoop.hbase.client.{ConnectionFactory, Get, Scan}
import org.apache.hadoop.hbase.util.Bytes
object hbase_test {

  val configuration = HBaseConfiguration.create()
  configuration.set("hbase.zookeeper.property.clientPort", "2181")
  configuration.set("hbase.zookeeper.quorum", "localhost")
  val connection = ConnectionFactory.createConnection(configuration)
  val admin = connection.getAdmin()
  val table = connection.getTable(TableName.valueOf("user_city"))
  val table2 = connection.getTable(TableName.valueOf("time_city"))
  def scanDataFromHTable(columnFamily: String, column: String) = {
    print(1)
    //定义scan对象
    val scan = new Scan()
    print(2)
    //添加列簇名称
    scan.addFamily(columnFamily.getBytes())
    print(3)
    //从table中抓取数据来scan
    val scanner = table.getScanner(scan)
    print(4)
    var result = scanner.next()
    print(result)
    //数据不为空时输出数据
    while (result != null) {
      println(s"rowkey:${Bytes.toString(result.getRow)},columnFamily:${columnFamily}:${column},value:${Bytes.toString(result.getValue(Bytes.toBytes(columnFamily), Bytes.toBytes(column)))}")
      result = scanner.next()
    }
    //通过scan取完数据后，记得要关闭ResultScanner，否则RegionServer可能会出现问题(对应的Server资源无法释放)
    scanner.close()
  }

  def close() = {
    if (connection != null) {
      try {
        connection.close()
        println("关闭成功!")
      } catch {
        case e: IOException => println("关闭失败!")
      }
    }
  }


  def main(args: Array[String]): Unit = {
//    val g = new Get("99".getBytes)
//    val result = table.get(g)
//    val value = Bytes.toString(result.getValue("info".getBytes,"city".getBytes))
//    println("GET 99 :"+value)
////    scanDataFromHTable("info", "city")
//    close()
//  }

    val configuration = HBaseConfiguration.create()
    configuration.set("hbase.zookeeper.property.clientPort", "2181")
    configuration.set("hbase.zookeeper.quorum", "localhost")
    val connection = ConnectionFactory.createConnection(configuration)//已经成功连接
    val admin = connection.getAdmin()

//    val table = connection.getTable(TableName.valueOf("user_ctiy"))
    val userTable = TableName.valueOf("time_city")
    //    //创建 user 表
    val tableDescr = new HTableDescriptor(userTable)
        tableDescr.addFamily(new HColumnDescriptor("info".getBytes))
        println("Creating table `time_city`. ")
        if (admin.tableExists(userTable)) {
              admin.disableTable(userTable)
              admin.deleteTable(userTable)
            }

        admin.createTable(tableDescr)
        println("Done!")
    //  }
  }
}
