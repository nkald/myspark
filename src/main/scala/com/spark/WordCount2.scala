package com.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object WordCount2 {
  com.spark.WordCount2
  def main(args: Array[String]): Unit = {
      val conf: SparkConf = new SparkConf().setAppName("My App")

      val context = new SparkContext(conf)

    val tuples: Array[(String, Int)] = context.textFile(args(0)).flatMap(_.split(" ")).groupBy(word => word)
      .map({case (word,value) => (word,value.size)}).collect()

    println(Array(1, 2, 3, 4, 5).iterator.length)

    println(tuples.mkString(","))
    context.stop()


    










  }

}
