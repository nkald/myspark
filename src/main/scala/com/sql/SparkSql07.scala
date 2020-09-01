package com.sql

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

object SparkSql07 {
  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("app")

    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    import spark.implicits._

    spark.sql("show tables").show()
    spark.sql("create table aa(id int)")

    spark.sql("show tables").show()
    spark.close()
  }
}
