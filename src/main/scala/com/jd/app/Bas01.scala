package com.jd.app

import org.apache.spark.rdd.RDD

object Bas01  extends BaseApp {
  override val outputPath: String = "output"

  def main(args: Array[String]): Unit = {
    runApp{

      val rdd: RDD[String] = sparkContext.textFile("input/user_visit_action.txt")

      val datas: RDD[Array[String]] = rdd.map(_.split("_"))

      val click: RDD[(String, Int)] = datas.filter(f=>f(6) != "-1").map(f=>(f(6),1))
      //点击量
      val clickCount: RDD[(String, Int)] = click.reduceByKey(_+_)

      val order: RDD[String] = datas.filter(f=>f(8) != "null").map(f=>f(8))

      //订单量
      val ordecount: RDD[(String, Int)] = order.flatMap(_.split(",")).groupBy(f=>f).map(f=>(f._1,f._2.size))

      //z支付数量
      val pay: RDD[String] = datas.filter(f=>f(10) != "null").map(f=>f(10))
      val paycount: RDD[(String, Int)] = pay.flatMap(_.split(",")).groupBy(f=>f).map(f=>(f._1,f._2.size))

      clickCount.join(ordecount).join(paycount).foreach(println)

    }

  }


}
