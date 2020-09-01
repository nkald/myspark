package com.jd.app

import org.apache.spark.rdd.RDD

object Bas02  extends BaseApp {
  override val outputPath: String = "output"

  def main(args: Array[String]): Unit = {
    runApp{

      val rdd: RDD[String] = sparkContext.textFile("input/user_visit_action.txt")

      val datas: RDD[Array[String]] = rdd.map(_.split("_"))

      val click: RDD[(String, Int)] = datas.filter(f=>f(6) != "-1").map(f=>(f(6),1))
      //点击量
      val clickCount: RDD[(String, (Int, Int, Int))] = click.reduceByKey(_+_).map(f=>(f._1,(f._2,0,0)))

      val order: RDD[String] = datas.filter(f=>f(8) != "null").map(f=>f(8))

      //订单量
      val ordecount: RDD[(String, (Int, Int, Int))] = order.flatMap(_.split(",")).groupBy(f=>f).map(f=>(f._1,f._2.size)).map(f=>(f._1,(0,f._2,0)))

      //z支付数量
      val pay: RDD[String] = datas.filter(f=>f(10) != "null").map(f=>f(10))
      val paycount: RDD[(String, (Int, Int, Int))] = pay.flatMap(_.split(",")).groupBy(f=>f).map(f=>(f._1,f._2.size)).map(f=>(f._1,(0,0,f._2)))

      val valueRdd = clickCount.union(ordecount).union(paycount).reduceByKey((f1,f2)=>(f1._1+f2._1,f1._2+f2._2,f1._3+f2._3))
      println("---------------------------------------")
      valueRdd.sortBy(_._2._1,false).take(10).foreach(println)
    }

  }


}
