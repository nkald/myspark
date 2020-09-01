package com.sql

import java.util.Properties

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

object SparkSql06 {
  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("app")

    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    import spark.implicits._


    val value: RDD[(String, Int)] = spark.sparkContext.makeRDD(List(
      ("zhangsan", 39),
      ("zhaoliu", 10)
    ))
    val frame: DataFrame = value.toDF("name","age")
    frame.write.format("jdbc")
      .option("url","jdbc:mysql://hadoop102:3306/test")
      .option("driver","com.mysql.jdbc.Driver")
      .option("user", "root")
      .option("password", "123123")
      .option("dbtable", "spark").mode("append").save()

//    val properties = new Properties
//    properties.setProperty("driver","com.mysql.jdbc.Driver")
//    properties.setProperty("user", "root")
//    properties.setProperty("password","123123")
//    spark.read.jdbc("jdbc:mysql://hadoop102:3306/test","spark",properties).show()
//
//    spark.read.format("jdbc")
//        .option("url","jdbc:mysql://hadoop102:3306/test")
//        .option("driver","com.mysql.jdbc.Driver")
//        .option("user", "root")
//        .option("password", "123123")
//        .option("dbtable", "spark")
//        .load().show


    spark.close()
  }
}
