package com.sparkSql

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object SparkSession2 {

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("app")


    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    val df: DataFrame = spark.read.json("input/employees.json")

    import spark.implicits._

/*
   val rdd: RDD[(String, Int)] =  spark.sparkContext.makeRDD(Array(
      ("zhang",19),
      ("wangwu",29),
      ("liuliu",39),
    ))

    val dataFrame: DataFrame = rdd.toDF("name","age")

    val dataset1: Dataset[Use] = dataFrame.as[Use]

    val frame: DataFrame = dataset1.toDF()

    println("+---------------------")
    frame.show()
    println("+---------------------")
    val rdd2: RDD[Row] = dataFrame.rdd
    rdd2.foreach(
      row => println(row(0),row(1))
    )


    val newrdd: RDD[Use] = rdd.map(data => {
      Use(data._1, data._2)
    })
     val dataset: Dataset[Use] = newrdd.toDS()

    val rdd1: RDD[Use] = dataset.rdd


    dataFrame.show()
    dataset.show()*/

    val unit: Dataset[Use] = df.as[Use]


  }



  case class Use(salary:BigInt,name:String)
}
