package com.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.util.{AccumulatorV2, LongAccumulator}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object Leijia {

  def main(args: Array[String]): Unit = {

    //使用累加器 实现word count

    val conf: SparkConf = new SparkConf().setMaster("local").setAppName("My App")

    val context = new SparkContext(conf)

    val list = List(("Hello",3) ,("hive",2), ("hbase",7), ("Hadoop",4), ("hive",5))

    val acc = new myL
    context.register(acc)

    val rdd: RDD[(String, Int)] = context.makeRDD(list)

    rdd.foreach(
      data => {
        acc.add(data)
      }
    )

    println(acc.value)
  }


  class myL extends AccumulatorV2[(String,Int),mutable.Map[String,Int]]{

    private var wordCountMap = mutable.Map[String,Int]()

    override def isZero: Boolean = {
      wordCountMap.isEmpty
    }

    override def copy(): AccumulatorV2[(String, Int), mutable.Map[String, Int]] = {
      new myL
    }

    override def reset(): Unit = {
      wordCountMap.clear()
    }

    override def add(v: (String, Int)): Unit = {
      val word = v._1
      val count = v._2
      val oldCount = wordCountMap.getOrElseUpdate(word,0)
      wordCountMap.put(word,count + oldCount)
    }

    override def merge(other: AccumulatorV2[(String, Int), mutable.Map[String, Int]]): Unit = {
      val map1 = wordCountMap
      val map2 = other.value

      wordCountMap = map1.foldLeft(map2)(
        (map,kv) => {
          val word = kv._1
          val count = kv._2
          val oldCount = map.getOrElse(word,0)
          map.put(word,count+oldCount)
          map
        })

    }

    override def value: mutable.Map[String, Int] = {
      wordCountMap
    }
  }
}
