package com.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object  Rdd{

  def main(args: Array[String]): Unit = {

    //配置文件

    val conf = new SparkConf().setAppName("myapp").setMaster("local[*]")
    //获取context
    val context = new SparkContext(conf)
    //
      val list = List(1,2,3,4,5)
//    context.textFile("input/txt.txt").saveAsTextFile("output")
//      val value: RDD[Int] = context.parallelize(list)
    val value: RDD[Int] = context.makeRDD(list,2).map(f=>f+1)
    value.saveAsTextFile("output")

      //关闭context
    context.stop


  }

}
