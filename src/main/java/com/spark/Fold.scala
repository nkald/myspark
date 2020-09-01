package com.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object Fold {

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setMaster("local").setAppName("My App")

    val context = new SparkContext(conf)

    val lis = List(("a", 88), ("b", 95), ("a", 91), ("b", 93), ("a", 95), ("b", 98))

    context.makeRDD(lis).combineByKey(
      a=>(a,1),
      (p:(Int,Int),v) => (p._1 + v,p._2 + 1),
     (x:(Int,Int),y:(Int,Int))=> ((x._2+y._2),(x._1 + y._1)
    ))





    val arr = Array(1,3,4,5,6,7)
    val arr12 = Array(1,3,4,5,6,7)
    val arr1001 = Array("1","3","4","5","6","7","x")

    val value1: RDD[Int] = context.makeRDD(arr)
    val value2: RDD[String] = context.makeRDD(arr1001)
//     val abc =  value1.intersection(value2)

    val dataRDD1 = context.makeRDD(List(1,2,3,4))
    val dataRDD2 = context.makeRDD(List(3,4,5,6))
    val dataRDD = dataRDD1.intersection(dataRDD2)




    println(context.makeRDD(arr, 3).distinct().glom().map(f => {
     f.max
    }).sum())

    val arr1 = Map(("a",1),("b",2),("c",3))
    val arr2 = Map(("a",1),("b",2),("c",3))

    arr1.foldRight(arr2)(
      (kv,map)=>{
        val key: String = kv._1
        val value:Int  = kv._2
        val newValue: Int = map.getOrElse(key,0)
        map.updated(key,value+newValue)
      }
    )


    arr1.foldLeft(arr2)(
      (map,kv)=>{
        val key: String = kv._1
        val value:Int  = kv._2
        val newValue: Int = map.getOrElse(key,0)
        map.updated(key,value+newValue)
      }
    ).foreach(println)


    val arr4 = Array(1,2,3,4)

    println(arr4.fold(0)(_ + _))
    println(arr4.foldLeft(0)(_ + _))


//    arr1.foldLeft(arr2)(
//      (map,kv)=> {
//        kv._1
//
//      }
//    )










    arr1.foldLeft(arr2)(
      (map,kv) => {
        val key: String = kv._1
        val value: Int = kv._2
        val avalue = arr2.getOrElse(key,0)
        map.updated(key,value + avalue)
      }
    ).foreach(println)
  }
}
