package com.project

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object LoadDatas {
  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("app")

    val spark: SparkSession = SparkSession.builder().enableHiveSupport().config(conf).getOrCreate()

//    spark.sql("create database atguigu")
    spark.sql("use atguigu")

    spark.sql(
      """
        |CREATE TABLE if not exists  `user_visit_action`(
        |  `date` string,
        |  `user_id` bigint,
        |  `session_id` string,
        |  `page_id` bigint,
        |  `action_time` string,
        |  `search_keyword` string,
        |  `click_category_id` bigint,
        |  `click_product_id` bigint,
        |  `order_category_ids` string,
        |  `order_product_ids` string,
        |  `pay_category_ids` string,
        |  `pay_product_ids` string,
        |  `city_id` bigint)
        |row format delimited fields terminated by '\t'
        |""".stripMargin)

    spark.sql(
      """
        |CREATE TABLE if not exists `product_info`(
        |  `product_id` bigint,
        |  `product_name` string,
        |  `extend_info` string)
        |row format delimited fields terminated by '\t'
        |""".stripMargin)

    spark.sql(
      """
        |CREATE TABLE if not exists `city_info`(
        |  `city_id` bigint,
        |  `city_name` string,
        |  `area` string)
        |row format delimited fields terminated by '\t'
        |""".stripMargin)


    spark.sql("load data local inpath 'input/user_visit_action.txt' into table user_visit_action")
    spark.sql("load data local inpath 'input/product_info.txt' into table product_info")
    spark.sql("load data local inpath 'input/city_info.txt' into table city_info")


    spark.sql("show tables").show()
    spark.sql("select * from  user_visit_action ").show()
    spark.sql("select * from  product_info ").show()
    spark.sql("select * from  city_info ").show()
    spark.close()
  }
}
