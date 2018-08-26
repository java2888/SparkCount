package com.test

import java.util.UUID
import org.apache.hadoop.hbase.{HBaseConfiguration, HTableDescriptor, TableName}
//import org.apache.hadoop.hbase.client.HBaseAdmin
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.spark._
//import org.apache.hadoop.hbase.client.HTable
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.mapreduce._
//import org.apache.hadoop.io._

object SparkCount {
  def initConfigurtion(): Unit = {

  }

  def static: Unit = {
    val tableName = "t1"
    val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.quorum", "Master,Slave1,Slave2")
    conf.set("hbase.zookeeper.property.clientPort", "2181")
    conf.set(TableInputFormat.INPUT_TABLE, tableName)
    conf.set(TableInputFormat.SCAN_COLUMNS, "apos:type")
  }

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf() //.setMaster("spark://testserverip:7077")
      .setAppName("reduce")
    val sc = new SparkContext(sparkConf)

    val tablename = "t1"
    val conf = HBaseConfiguration.create()
    //设置zooKeeper集群地址，也可以通过将hbase-site.xml导入classpath，但是建议在程序里这样设置
    conf.set("hbase.zookeeper.quorum", "Master,Slave1,Slave2")
    //设置zookeeper连接端口，默认2181
    conf.set("hbase.zookeeper.property.clientPort", "2181")
    conf.set(TableInputFormat.INPUT_TABLE, tablename)
    conf.set(TableInputFormat.SCAN_COLUMNS, "apos:type")


    //读取数据并转化成rdd
    val hBaseRDD = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result])

    val count = hBaseRDD.count()
    println("count=" + count)
    println("---------------")
    hBaseRDD.foreach { case (_, result) => {
      println("**********")
      //获取行键
      val key = Bytes.toString(result.getRow)
      //通过列族和列名获取列
      val typenames = Bytes.toString(result.getValue("apos".getBytes, "type".getBytes))
      println("key=[" + key + "]; typenames=[" + typenames + "]")
      if (key != null && typenames != null) {
        println(key + ":" + typenames);
      }
      result.toString()
      println("end.&&&&&&&&&&&")
    }
    }
    println("map begin");
    val result = hBaseRDD.map(tuple => Bytes.toString(tuple._2.getValue("apos".getBytes, "type".getBytes))).map(s => (s, 1)).reduceByKey((a, b) => a + b)
    println("map end");

    //最终结果写入hdfs，也可以写入hbase   result.saveAsTextFile("hdfs://localhost:9070/user/root/aposStatus-out")

    //也可以选择写入hbase,写入配置
    var resultConf = HBaseConfiguration.create()
    //设置zooKeeper集群地址，也可以通过将hbase-site.xml导入classpath，但是建议在程序里这样设置
    resultConf.set("hbase.zookeeper.quorum", "Master,Slave1,Slave2")
    //设置zookeeper连接端口，默认2181
    resultConf.set("hbase.zookeeper.property.clientPort", "2181")
    //注意这里是output
    resultConf.set(TableOutputFormat.OUTPUT_TABLE, "count-result")
    var job = Job.getInstance(resultConf)
    job.setOutputKeyClass(classOf[ImmutableBytesWritable])
    job.setOutputValueClass(classOf[org.apache.hadoop.hbase.client.Result])
    job.setOutputFormatClass(classOf[TableOutputFormat[ImmutableBytesWritable]])
    val hbaseOut = result.map(tuple => {
      val put = new Put(Bytes.toBytes(UUID.randomUUID().toString))
      put.addColumn(Bytes.toBytes("result"), Bytes.toBytes("type"), Bytes.toBytes(tuple._1))
      //直接写入整型会以十六进制存储
      put.addColumn(Bytes.toBytes("result"), Bytes.toBytes("count"), Bytes.toBytes(tuple._2 + ""))
      (new ImmutableBytesWritable, put)
    })
    hbaseOut.saveAsNewAPIHadoopDataset(job.getConfiguration)
    sc.stop()

  }

}

