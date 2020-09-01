package com.sparkSql

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object SparkSession1 {

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("app")


    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    val frame: DataFrame = spark.read.json("input/employees.json")

    frame.createTempView("mytable")





    //1.sql
    spark.sql("select * from mytable").show()
    println("---------------------------------------------------")
    spark.sql("select salary + 100 from mytable").show()
    spark.sql("select avg(salary) from mytable").show()
    //2.dsl
    import spark.implicits._
//    frame.select("name","salary").show()

//      frame.select($"salary" + 1000).show()
//      frame.select('name + "abc").show()

    val rdd: RDD[(String, Int)] = spark.sparkContext.makeRDD(List(
      ("ls", 12),
      ("ww", 18)
    ))
    val frame1: DataFrame = rdd.toDF()

    frame1.show()

    //Row
    val rdd1: RDD[Row] = frame1.rdd

    rdd1.foreach(
      data=>{
        println(data(0))
        println(data(1))
      }

    )

    val value: RDD[Use] = rdd.map(t => {
      Use(t._1, t._2)
    })

    val useds: Dataset[Use] = value.toDS()
    println("----------------------")
    useds.show()
    val frame2: DataFrame = value.toDF()

    val rdd2: RDD[Use] = useds.rdd



    spark.stop()



  }

  case class Use(name:String,age:Int)
}
