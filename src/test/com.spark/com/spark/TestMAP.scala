package com.spark

import org.apache.spark.{SparkConf, SparkContext}

object TestMAP {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
    conf.setAppName("MY").setMaster("local[*]")

    val context = new SparkContext(conf)


    val ints = Array(1,2,3,4)
    //12   a1  b1  a2  b2
    //34  a3   b3   b4

    val rdd = context.makeRDD(ints,2)

    val rdd1 = rdd.map(f=>{
      println("a " + f)
      f
    })
    val rdd2 = rdd1.map(f=> {println("b " + f)
      f
    })

    rdd2.collect().foreach(println)


  }

}
