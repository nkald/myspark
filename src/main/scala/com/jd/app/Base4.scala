package com.jd.app

import java.io

import org.apache.spark.rdd.RDD

object Base4 extends BaseApp {
  override val outputPath: String = "output"

  def main(args: Array[String]): Unit = {

    runApp{

      //分别统计每个品类点击的次数，下单的次数和支付的次数：
      val odsrdd: RDD[String] = sparkContext.textFile("input/user_visit_action.txt")
      //每个品类点击的次数
      val rdd: RDD[(String, (Int, Int, Int))] = odsrdd.flatMap {
        line => {
          val str = line.split("_")
          if (str(6) != "-1") {
            List((str(6), (1, 0, 0)))
          } else if (str(8) != "null") {
            val orders: Array[String] = str(8).split(",")
            orders.map(f =>
              (f, (0, 1, 0)))
          } else if (str(10) != "null") {

            val orders1 = str(10).split(",")

            orders1.map(f =>
           (f, (0, 0, 1)))

          } else {
            Nil
          }
        }
      }
//      rdd.collect().foreach(println)
      rdd.reduceByKey((f1,f2)=>(f1._1 + f2._1,f1._2+ f2._2,f1._3+f2._3)).sortBy(_._2._1,false).collect().foreach(println)
    }
  }
}
