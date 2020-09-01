package com.jd.app

import org.apache.spark.rdd.RDD
import org.apache.spark.util.AccumulatorV2

import scala.collection.mutable

object Bas05 extends BaseApp {
  override val outputPath: String = "outpunt"
  /*
  思路：1.给点击事件打标记
        2.累加器
        3.把事件封装成属性
        4.累加器add
        5.累加器 moger
   */

  def main(args: Array[String]): Unit = {
    val acc = new Leijiaqi


    runApp {
      sparkContext.register(acc)
      val result: RDD[String] = sparkContext.textFile("input/user_visit_action.txt")

      val ods: RDD[Array[String]] = result.map(f => f.split("_"))
      ods.foreach {
        line => {
          if (line(6) != "-1") {
            acc.add(line(6), "click")
          } else if (line(8) != "null") {
            line(8).split(",").foreach(
              ord => acc.add(ord, "order")
            )
          } else if (line(10) != "null") {
            line(10).split(",").foreach(
              pay => acc.add(pay, "pay"))
          }
        }
      }

      val res = acc.value.map(f => f._2).toList.sortWith((x, y) => {
        x.clickCount > y.clickCount
      }).map(f=>(f.id))take(10)

      res.foreach(println)

      val rdd: RDD[Array[String]] = sparkContext.textFile("input/user_visit_action.txt")
        .map(_.split("_")).filter(line => line(6) != "-1" && res.contains(line(6)))

      val rdd2 = rdd.map(line => {
          (line(6), (line(2)))
      })
      rdd2.groupByKey().map(data => {
        val stringToStrings: Map[String, Iterable[String]] = data._2.groupBy(f=>f)
        val stringToInt: Map[String, Int] = stringToStrings.map(p=>(p._1,p._2.size))
        (data._1,stringToInt.toList.sortWith((x,y)=>x._2 > y._2).take(10))
      }).foreach(println)

    }
  }

  // in:(id,标记)
  //out : MAP(id,对象（dianjishuxing）)
  class Leijiaqi extends AccumulatorV2[(String, String), mutable.Map[String, MyBean]] {
    //value
    var hotmap = mutable.Map[String, MyBean]()

    override def isZero: Boolean = hotmap.isEmpty  //判断是否为空

    override def copy(): AccumulatorV2[(String, String), mutable.Map[String, MyBean]] = new Leijiaqi  //拷贝

    override def reset(): Unit = hotmap.clear()  //重置

    override def add(input: (String, String)): Unit = {   //添加元素规则
      val key: String = input._1

      val bean: MyBean = hotmap.getOrElse(key, MyBean(key, 0, 0, 0))

      input match {
        case (_, "click") => bean.clickCount += 1
        case (_, "order") => bean.orderCount += 1
        case (_, "pay") => bean.payCount += 1
      }
      hotmap(key) = bean
    }
    //元素添加规则
    override def merge(other: AccumulatorV2[(String, String), mutable.Map[String, MyBean]]): Unit = {//累加器之间的合并规则
      other.value.foldLeft(hotmap)(
        (map1, kv) => {
          val key: String = kv._1
          val va: MyBean = kv._2

          val bean: MyBean = map1.getOrElse(key, MyBean(key, 0, 0, 0))

          bean.clickCount += va.clickCount
          bean.orderCount +=va.orderCount

          bean.payCount += va.payCount

          map1(key) = bean
          map1
        }
      )
    }
  //累加器与累加器 合并规则

    override def value: mutable.Map[String, MyBean] = hotmap   //设置value 值
  }

}


