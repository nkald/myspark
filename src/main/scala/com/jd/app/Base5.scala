package com.jd.app

import org.apache.spark.rdd.RDD
import org.apache.spark.util.AccumulatorV2

import scala.collection.mutable

object Base5 extends BaseApp {
  override val outputPath: String = "output"

  def main(args: Array[String]): Unit = {
    val acc = new HotCategoryAccumulator


    runApp {
      sparkContext.register(acc)
      //分别统计每个品类点击的次数，下单的次数和支付的次数：
      val odsrdd: RDD[String] = sparkContext.textFile("input/user_visit_action.txt")
      odsrdd.foreach( action => {
        val datas: Array[String] = action.split("_")
        if(datas(6) != "-1"){
          acc.add(datas(6),"click")

        } else if (datas(8) != "null") {
          val ids: Array[String] = datas(8).split(",")
          ids.foreach(
            id => {
              acc.add(id,"order")
            }
          )
        } else if  (datas(10) != "null"){
          val ids: Array[String] = datas(8).split(",")
          ids.foreach(
            id => {
              acc.add(id,"pay")
            }
          )
        }
      })


      val accValue: mutable.Map[String, HotJD] = acc.value
      accValue.map(_._2).toList.sortWith(
        (left,right) => {
          if (left.clickcount > right.clickcount) true
          else if (left.clickcount == right.clickcount) {
            left.orderclickcount > right.orderclickcount
          }else false
        }
      ).foreach(println)
    }
  }
}

/*
In :
Out:HotJD
 */
class HotCategoryAccumulator extends  AccumulatorV2[(String,String),mutable.Map[String,HotJD]]{
   val hostCateroryMap =  mutable.Map[String,HotJD]()
  override def isZero: Boolean = hostCateroryMap.isEmpty

  override def copy(): AccumulatorV2[(String, String), mutable.Map[String, HotJD]] = {
    new HotCategoryAccumulator
  }

  override def reset(): Unit = hostCateroryMap.clear()

  override def add(v: (String, String)): Unit = {
    val categorruId = v._1
    val actionType  = v._2

    val hot = hostCateroryMap.getOrElse(categorruId,HotJD(categorruId,0,0,0))

    actionType match {
      case "click" =>   hot.clickcount += 1
      case "order" =>   hot.orderclickcount += 1
      case "pay" =>     hot.paycount += 1
    }
    hostCateroryMap(categorruId) = hot
  }

  override def merge(other: AccumulatorV2[(String, String), mutable.Map[String, HotJD]]): Unit = {
    other.value.foreach{
      case ( categoruId,otherHc) => {
        val thisHc: HotJD = hostCateroryMap.getOrElse(categoruId,HotJD(categoruId,0,0,0))

        thisHc.clickcount += otherHc.clickcount
        thisHc.orderclickcount += otherHc.orderclickcount
        thisHc.paycount += otherHc.paycount

        hostCateroryMap(categoruId) = thisHc
      }
    }

  }

  override def value: mutable.Map[String, HotJD] = hostCateroryMap
}


case class HotJD(
             var   cateid:String,
             var   clickcount:Int,
             var  orderclickcount:Int,
             var   paycount:Int
                )