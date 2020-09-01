package com.project

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object LoadDatas1 {
  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("app")

    val spark: SparkSession = SparkSession.builder().enableHiveSupport().config(conf).getOrCreate()

//    spark.sql("create database atguigu")
    spark.sql("use atguigu")
    spark.sql(
      """
        |select
        |tb3.area ,tb3.click_product_id,tb3.clickcount
        |from (
        |     select
        |tb2.area,tb2.click_product_id ,tb2.clickcount,ROW_NUMBER()  over (partition by tb2.area order by tb2.clickcount desc ) ro
        |from (
        |     select
        |tb1.click_product_id,city_info.area,count(*) clickcount
        |from (
        |     select click_product_id,city_id from user_visit_action where click_product_id != -1
        |         ) tb1  join city_info  on tb1.city_id = city_info.city_id  group by tb1.click_product_id, city_info.area
        |         ) tb2
        |         )tb3  where tb3.ro <= 3
        |""".stripMargin
    ).show

    spark.close()
  }
}
