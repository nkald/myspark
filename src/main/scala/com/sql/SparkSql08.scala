package com.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object SparkSql09 {
  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("sql")

    val spark: SparkSession = SparkSession.builder().enableHiveSupport().config(conf).getOrCreate()

    spark.sql("show databases").show()
    spark.sql("use mysql")

    spark.sql("show tables").show()
    spark.close()
  }
}
