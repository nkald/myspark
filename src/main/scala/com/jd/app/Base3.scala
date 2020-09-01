package com.jd.app

import com.jd.app.Base2.sparkContext
import org.apache.spark.rdd.RDD

object Base3 extends BaseApp {
  override val outputPath: String = "output"

  def main(args: Array[String]): Unit = {

    runApp{

      //分别统计每个品类点击的次数，下单的次数和支付的次数：
      val odsrdd: RDD[String] = sparkContext.textFile("input/user_visit_action.txt")
      //每个品类点击的次数
      val clicked = odsrdd.map(line => {
        val arr: Array[String] = line.split("_")
        (arr(6), 1)
      }).filter(f => f._1 != "-1").reduceByKey(_ + _).mapValues(f=>(f,0,0))

      //下单的次数
      val order = odsrdd.map(line => {
        val arr: Array[String] = line.split("_")
        arr(8)
      }).filter(_ != "null").map(f => f.split(",")).flatMap(f => f).groupBy(f => f)
        .map(x => (x._1, x._2.size)).mapValues(f=>(0,f,0))
      //支付的次数：

      val pay = odsrdd.map(line => {
        val arr: Array[String] = line.split("_")
        arr(10)
      }).filter(_ != "null").map(f => f.split(",")).flatMap(f => f).groupBy(f => f)
        .map(x => (x._1, x._2.size)).mapValues(f=>(0,0,f))
      (5, ((6011, 1820), 1132))
      //拼接
      clicked.union(order).union(pay).reduceByKey{
        case (a:(Int,Int,Int),b:(Int,Int,Int)) => (a._1+b._1,a._2+b._2,a._3+b._3)
      }.take(10).foreach(println)

    }
  }
}
