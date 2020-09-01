package com.jd.app

import org.apache.spark.rdd.RDD
import org.apache.spark.util.AccumulatorV2

import scala.collection.mutable

object Bas06 extends BaseApp {
  override val outputPath: String = "outpunt"
  /*
  思路：1.给点击事件打标记
        2.累加器
        3.把事件封装成属性
        4.累加器add
        5.累加器 moger
   */

  def main(args: Array[String]): Unit = {

    runApp {

      val result: RDD[String] = sparkContext.textFile("input/user_visit_action.txt")
      val ods: RDD[Array[String]] = result.map(f => f.split("_"))

      val fenmuTongji: RDD[(String, Int)] = ods.map(line =>line(3)).groupBy(f=>f).map(f=>(f._1,f._2.size))
      val fengmu: Map[String, Int] = fenmuTongji.collect().toMap
      ods.map(line => {
        (line(2),(line(3),line(4)))
      }).groupByKey().map(data => {
        data._2.toList.sortWith((f1,f2)=> f1._2 < f2._2).map(f=>f._1)
      }).map(fs=>fs.sliding(2).toList).filter(f=>f.length>1).flatMap(f=>f).groupBy(f=>f).map(f=>(f._1,f._2.size)).map{data=>{

       val result =  data._2.toDouble / fengmu.get(data._1(0)).get
        s"跳转 ${data._1(0)} - ${data._1(1)} --> ${result*100}%"
      }}.saveAsTextFile("outputmydianji")
    }
  }


}


