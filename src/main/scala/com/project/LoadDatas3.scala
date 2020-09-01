package com.project

import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.{Encoder, Encoders, SparkSession, functions}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object LoadDatas3 {
  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("app")

    val spark: SparkSession = SparkSession.builder().enableHiveSupport().config(conf).getOrCreate()

    //    spark.sql("create database atguigu")
    spark.sql("use atguigu")

    //    user_visit_action

    val udaf = new MyUdaf()
    spark.udf.register("MyUdaf", functions.udaf(udaf))


//    spark.sql(
//      """
//        |         select tb1.product_name,
//        |                tb1.area,
//        |                count(*) clickCount,
//        |                MyUdaf(city_name)
//        |
//        |         from (
//        |                  select product_name, area, city_name
//        |
//        |                  from user_visit_action
//        |                             join product_info pro
//        |                                on user_visit_action.click_product_id = pro.product_id join city_info ci
//        |                                    on user_visit_action.city_id = ci.city_id
//        |              ) tb1
//        |         group by tb1.product_name, tb1.area
//        |""".stripMargin).show()





        spark.sql(
          """
            |select
            |        t2.product_name,
            |       t2.area,
            |       t2.clickCount,
            |       ud,
            |        ROW_NUMBER() over(partition by product_name,area order by t2.clickCount desc ) ronum
            |from (
            |         select tb1.product_name,
            |                tb1.area,
            |                count(*) clickCount,
            |                MyUdaf(city_name) ud
            |
            |         from (
            |                  select product_name, area, city_name
            |
            |                  from user_visit_action
            |                             join product_info pro
            |                                on user_visit_action.click_product_id = pro.product_id join city_info ci
            |                                    on user_visit_action.city_id = ci.city_id
            |              ) tb1
            |         group by tb1.product_name, tb1.area
            |     ) t2
            |""".stripMargin).createTempView("tb3")



        spark.sql(
          """
            |select
            |area,
            |product_name,
            |clickCount,
            |ud
            |from  tb3 where ronum <= 3
            |""".stripMargin).show()
        spark.stop()

  }
  case class Buff( var toatCount:Long , var city: mutable.Map[String,Long])
  //in string
  //buffer
  //out String
  class MyUdaf extends Aggregator[String,Buff,String]{
    override def zero: Buff = new Buff(0L,mutable.Map[String,Long]())

    override def reduce(buffer: Buff, city: String): Buff = {
      buffer.toatCount += 1
      val city1: mutable.Map[String, Long] = buffer.city

      buffer.city.update(city,city1.getOrElse(city,0L) + 1)

      buffer
    }

    override def merge(b1: Buff, b2: Buff): Buff = {
      val cityMap: mutable.Map[String, Long] = b1.city
      val kv: mutable.Map[String, Long] = b2.city
      b1.toatCount += b2.toatCount
      b1.city = cityMap.foldLeft(kv){
       case (map,(k,v)) => {
          val newClick: Long = map.getOrElse(k,0L) + v
          map.update(k,newClick)
          map
        }
      }
      b1
    }

    override def finish(reduction: Buff): String = {
      val count: Long = reduction.toatCount
      val city: mutable.Map[String, Long] = reduction.city

      val newCount: List[(String, Long)] = city.toList.sortWith((left,right)=>left._2 > right._2).take(2)

      var list = ListBuffer[String]()
      var sum = 0L
      newCount.foreach{
        case (city,click) => {
          val bil: Long = click * 100 / count
          list.append(city+" "+ bil + "%")
          sum += bil
        }
      }
      if (city.size > 2) {
        list.append("其他"+ (100 - sum ) +"%" )
      }
      list.mkString(",")
    }

    override def bufferEncoder: Encoder[Buff] = Encoders.product


    override def outputEncoder: Encoder[String] = Encoders.STRING
  }

}
