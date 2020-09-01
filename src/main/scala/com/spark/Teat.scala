package com.spark

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.{Partitioner, SparkConf, SparkContext}
import org.junit.Test

object Teat {

  def main(args: Array[String]): Unit = {

    val context  = new SparkContext(new SparkConf().setAppName("myapp").setMaster("local[*]"))
    val system = FileSystem.newInstance(new Configuration())
    system.delete(new Path("output"),true)


      val arr = List(
        ("nba", "xxx"),
        ("cba", "xxx"),
        ("wnba", "xxx"),
        ("nba", "xxx"),
        ("cba", "xxx")
      )
      context.makeRDD(arr).partitionBy(new mypartition).saveAsTextFile("output")
      context.makeRDD(arr).partitionBy(new mypartition).saveAsTextFile("output")

    context.stop()

  }

}

class mypartition extends Partitioner{
  override def numPartitions = 3

  override def getPartition(key: Any) = {
    key match {
      case "nba" => 0
      case "cba" => 1
      case "wnba" => 2
      case _ => 2
    }


  }
}