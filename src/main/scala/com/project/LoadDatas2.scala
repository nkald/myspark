package com.project

import org.apache.spark.SparkConf
import org.apache.spark.sql.{Encoder, Encoders, SparkSession, functions}
import org.apache.spark.sql.expressions.{Aggregator, UserDefinedAggregateFunction}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object LoadDatas2 {
  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("app")

    val spark: SparkSession = SparkSession.builder().enableHiveSupport().config(conf).getOrCreate()
    import spark.implicits._

    //    spark.sql("create database atguigu")
    spark.sql("use atguigu")

    spark.sql(
      """
        |select *
        |from (select click_product_id, city_id
        |      from user_visit_action
        |      where click_product_id != -1) t5
        |     join product_info on click_product_id = product_info.product_id
      """.stripMargin).createTempView("tb1")

    val cityRemark = new CityRemarK()

    spark.udf.register("cityRemark",functions.udaf(cityRemark))
    spark.sql(
      """
        |    select tb1.product_name,
        |            city_info.area,
        |            count(*) clickcount,
        |                cityRemark(city_name) hah
        |   from tb1
        |   join city_info on tb1.city_id = city_info.city_id
        |   group by tb1.product_name, city_info.area
        |""".stripMargin).createTempView("tb2")


    spark.sql(
      """
         |select tb2.area,hah,
         |                tb2.product_name,
         |                tb2.clickcount,
         |                ROW_NUMBER() over (partition by tb2.area order by tb2.clickcount desc ) ro
         |
         |
         |         from  tb2
        |""".stripMargin
    ).createTempView("tb3")

    spark.sql("select * from tb3").show()

    spark.sql(
      """
        |select tb3.area,
        |       tb3.product_name,
        |       tb3.clickcount,hah
        |from  tb3
        |where tb3.ro <= 3;
        |""".stripMargin)

//    spark.sql("select * from tb3 ").show
    spark.close()
  }
  case class Buff(var totalcunt:Long,var cityMap:mutable.Map[String,Long])

  //IN        String --
  //buff      map[cityname,clickCount] ,toutlcount
  //out         String

//  class TEST extends UserDefinedAggregateFunction
  class CityRemarK extends Aggregator[String,Buff,String] {

    override def zero: Buff = Buff(0L,mutable.Map[String,Long]())

    override def reduce(buffer: Buff, cityName: String): Buff = {
      //点击跟新数量
      buffer.totalcunt += 1
      //更新对应城市的点击数量

      val newCount: Long = buffer.cityMap.getOrElse(cityName,0L) + 1
      buffer.cityMap.update(cityName,newCount)
      buffer
    }

    override def merge(buff1: Buff, buff2: Buff): Buff = {
      val allCount: Long = buff1.totalcunt + buff2.totalcunt
      val map1: mutable.Map[String, Long] = buff1.cityMap
      val map2: mutable.Map[String, Long] = buff2.cityMap
      val allClick: mutable.Map[String, Long] = map1.foldLeft(map2){
        case (map,(k,v))=>{
          val newClick: Long = map.getOrElse(k,0L) + v
          map.update(k,newClick)
          map
        }
      }
      buff1.cityMap = allClick
      buff1.totalcunt = allCount
      buff1
    }
    override def finish(reduction: Buff): String = {
      val totalcunt: Long = reduction.totalcunt
      val cityMap: mutable.Map[String, Long] = reduction.cityMap
      val cityTOcount: List[(String, Long)] = cityMap.toList.sortWith((x,y)=>x._2 > y._2).take(2)

      val hashRes = cityMap.size > 2

      val list = ListBuffer[String]()
      var sum = 0L
      cityTOcount.foreach{
        case (city,cnt) => {
          val r = (cnt * 100 / totalcunt)
          list.append(city + "" + r +"%")
          sum += r
        }
      }
      if (hashRes) {
        list.append("其他" + (100 - sum ) + "%")
      }
      list.mkString(",")
    }

    override def bufferEncoder: Encoder[Buff] = Encoders.product

    override def outputEncoder: Encoder[String] = Encoders.STRING
  }


}
