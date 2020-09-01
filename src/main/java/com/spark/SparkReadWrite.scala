package com.spark

import org.apache.spark.{SparkConf, SparkContext}

object SparkReadWrite {

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setMaster("local").setAppName("My App")

    val context = new SparkContext(conf)


  }

}
