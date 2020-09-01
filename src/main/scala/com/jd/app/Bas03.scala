package com.jd.app

import org.apache.spark.rdd.RDD

object Bas03  extends BaseApp {
  override val outputPath: String = "output"

  def main(args: Array[String]): Unit = {
    runApp{

      val rdd: RDD[String] = sparkContext.textFile("input/user_visit_action.txt")

      val datas: RDD[Array[String]] = rdd.map(_.split("_"))

      val rdd1: RDD[(String, (Int, Int, Int))] = datas.flatMap{
        line => {
          if (line(6) != "-1"){
            val str: String = line(6)
            List((line(6),(1,0,0)))
          } else if (line(8) != "null"){
            line(8).split(",").map(f=>(f,(0,1,0)))
          }else if (line(10) != "null") {
            line(10).split(",").map(f=>(f,(0,0,1)))
          } else Nil
        }
      }

      rdd1.reduceByKey{
        case (f1,f2) => (f1._1+f2._1,f1._2+f2._2,f1._3+f1._3)
      }.sortBy(f=>f._2._1,false).take(10).foreach(println)


    }

  }


}
