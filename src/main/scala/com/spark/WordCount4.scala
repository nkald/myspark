package com.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object WordCount4 {


  def main(args: Array[String]): Unit = {

    //saprk 配置文件
    val conf: SparkConf = new SparkConf().setMaster("yarn").setAppName("myapp")
    // new SparkContext 对象
    val sc = new SparkContext(conf)
    //textFile 将文件一行一行的读取
    val txt: RDD[String] = sc.textFile("./input")

    val result: Array[(String, Int)] = txt.flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_).collect()

    println(result.mkString(","))
    //滚逼链接
   sc.stop()

  }
}
