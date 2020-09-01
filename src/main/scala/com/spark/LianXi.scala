package com.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


object LianXi {


  def main(args: Array[String]): Unit = {

      val conf: SparkConf = new SparkConf().setMaster("local").setAppName("My App")

      val context = new SparkContext(conf)

      val Rdd: RDD[String] = context.textFile("input/agent.log")

    Rdd.map(line=>{

      val file: Array[String] = line.split(" ")
      ((file(1),file(4)),1)
    })

    //时间戳，省份，城市，用户，广告
    //统计出每一个省份每个广告被点击数量排行的Top3
    //1.取出需要的数据

    //2.求和
        .reduceByKey(_+_)
    //
        .map{
          case ((prv,adv),sum) =>(prv,(adv,sum))
        }.groupByKey().mapValues(iter => iter.toList.sortWith(
      (left,right) => left._2>right._2
    ).take(3)).collect().foreach(println)



      context.stop()













  }

}
