package com.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


object WordCount {


  def main(args: Array[String]): Unit = {
      val conf: SparkConf = new SparkConf().setMaster("local").setAppName("My App")

      val context = new SparkContext(conf)

      val rdd: RDD[String] = context.textFile("input/txt.txt")

    val arr = Array(1,2,3,4,5,6,7,8)


//    val rdd1: RDD[Int] = context.makeRDD(arr,2)
//     rdd.flatMap(f=>f.split(" ")).map(f=>(f,1)).reduceByKey(_+_).collect().foreach(println)

////    rdd1.mapPartitions(f=>f.filter(x=> x%2 ==0)).collect().foreach(println)
////    rdd1.mapPartitionsWithIndex{
////      (a,b)=>{
////            b.map(x=>(x,a))
////      }
////    }.collect().foreach(println)
//
//   val array: Array[Array[Int]] = rdd1.glom().collect()
//    array.foreach(arr => (println(arr.mkString(",")) ))
//    val tuples = Array(Array(1,2),Array(2,3))
//    tuples.flatten
    context.makeRDD(arr,2).sample(false,0.5).collect().foreach(println)

//    rdd.flatMap(f=>f.split(" ")).groupBy(f=>f).map(f=>(f._1,f._2.size)).collect().foreach(println)

      context.stop()













  }

}
