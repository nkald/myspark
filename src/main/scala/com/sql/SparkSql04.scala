package com.sql

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, LongType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object SparkSql04 {
  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("app")


    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    val df: DataFrame = spark.read.json("input/employees.json")
    val d1f: DataFrame = spark.read.format("json").load("input/employees.json")
    spark.read.csv("input/people.csv").show()
    spark.read.format("csv")
      .option("sep", ";")
      .option("inferSchema", "true")
      .option("header", "true")
      .load("input/people.csv").show()

//    d1f.show()
//
//    df.write.format("json").save("ouput01")
//    df.write.format("json").mode("append").save("ouput01")

    spark.close()
  }
}
