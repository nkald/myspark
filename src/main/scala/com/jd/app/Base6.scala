package com.jd.app

import org.apache.spark.rdd.RDD
import org.apache.spark.util.AccumulatorV2

import scala.collection.mutable

object Base6 extends BaseApp {
  override val outputPath: String = "output"

  def main(args: Array[String]): Unit = {
    val acc = new Rejust
    runApp{
      sparkContext.register(acc)
      val odsRdd: RDD[String] = sparkContext.textFile("input/user_visit_action.txt")
      val datas: RDD[Array[String]] = odsRdd.map(_.split("_"))
      datas.foreach{  //给所有行为打标签
        line => {
          if ( line(6) != "-1") {
            acc.add(line(6),"click")
          }else if (line(8) != "null") {
            val orders: String = line(8)
            orders.split(",")foreach(
              or => acc.add(or,"order")
            )
          } else if (line(10) != "null"){
            val pays: Array[String] = line(10).split(",")
            pays.foreach( f =>
              acc.add( f,"pay")
            )
          }
        }
      }
    }
    val value: mutable.Map[String, TestBean] = acc.value
    println(value)
    value.map(_._2).toList.sortWith((t1,t2) => t1.clickCount > t2.clickCount).take(10).foreach(println)
  }
}

// int  (String,String)  //ID,标记
// out  umtable.Map     // mutable.Map[String,TestBean]  //id ,bean -封装后的数据
class Rejust extends  AccumulatorV2[(String,String),mutable.Map[String,TestBean]] {

  val hostMap = mutable.Map[String,TestBean]()

  override def isZero: Boolean = hostMap.isEmpty

  override def copy(): AccumulatorV2[(String, String), mutable.Map[String, TestBean]] = new Rejust

  override def reset(): Unit = hostMap.clear()

  override def add(value: (String, String)): Unit = {

    val key: String = value._1

    //获取map中的value 没给默认值
    val newValue: TestBean = hostMap.getOrElse(key,TestBean(key,0,0,0))

    value match {
          case (_,"click") => newValue.clickCount += 1
          case (_,"order") => newValue.orderCount += 1
          case (_,"pay") => newValue.payCount += 1
        }
    hostMap.update(key,newValue)
  }

  override def merge(other: AccumulatorV2[(String, String), mutable.Map[String, TestBean]]): Unit = {
    val map1: mutable.Map[String, TestBean] = other.value

    map1.foldLeft(hostMap){
      (map,kv) => {
        val key: String = kv._1
        val re          = kv._2

        val bean: TestBean = map.getOrElse(key,TestBean(key,0,0,0))

        bean.clickCount += re.clickCount

        bean.payCount += re.payCount

        bean.orderCount += re.orderCount
        hostMap(key) = bean
        hostMap
      }
    }
  }

  override def value: mutable.Map[String, TestBean] = hostMap
  }

case class TestBean(
                   var id:String,
                   var clickCount:Int,
                   var orderCount:Int,
                   var payCount:Int
                   )