package com.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object WordCount3 {


  def main(args: Array[String]): Unit = {
//    val conf: SparkConf = new SparkConf().setMaster("local").setAppName("myapp")
//    //
//    val sc = new SparkContext(conf)
//
//    val txt: RDD[String] = sc.textFile("./input")
//    val result: Array[(String, Int)] = txt.flatMap(_.split(" ")).groupBy(word => word).map({
//      case (a, b) => (a, b.size)
//    }).collect()
//    println(result.mkString(","))
//   sc.stop()
    val ints = Array(1,2,3,4,5)
    println(ints.slice(0, 3).mkString(","))
  }
}
