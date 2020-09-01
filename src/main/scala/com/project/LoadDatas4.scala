package com.project

import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.{Aggregator, MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, LongType, MapType, StringType, StructField, StructType}
import org.apache.spark.sql.{Encoder, Encoders, Row, SparkSession, functions}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object LoadDatas4 {
  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("app")

    val spark: SparkSession = SparkSession.builder().enableHiveSupport().config(conf).getOrCreate()

    spark.sql("use atguigu")



    val udaf = new MyUdaf()
    spark.udf.register("MyUdaf", functions.udaf(udaf))



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
            |ud,ronum
            |from  tb3 where ronum <= 3
            |""".stripMargin).show()
        spark.stop()
  }
  case class Buff( var toatCount:Long , var city: mutable.Map[String,Long])
//  in string
//  buffer
//  out String
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

//  class MyUDAF extends UserDefinedAggregateFunction{
//    override def inputSchema: StructType = StructType(Array(StructField("city",StringType)))
//
//    override def bufferSchema: StructType = StructType(Array(StructField("map",MapType(StringType,LongType)),StructField("totalCount",LongType)))
//
//    override def dataType: DataType = StringType
//
//    override def deterministic: Boolean = true
//
//    override def initialize(buffer: MutableAggregationBuffer): Unit = {
//      buffer(0)= mutable.Map[String,Long]()
//      buffer(1)= 0L
//    }
//    override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
//      val count: Long = buffer.getLong(1) + 1
//      val map: collection.Map[String,Long] = buffer.getMap[String,Long](0)
//      val str: String = input.getString(0)
//      val l: Long = map.getOrElse(str,0L) + 1L
//      val stringToLong: collection.Map[String, Long] = map.updated(str,l)
//
//      buffer.update(0,stringToLong)
//      buffer.update(1,count)
//    }
//    override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
//
//      val sun1: Long = buffer1.getLong(1)
//      val sun2: Long = buffer2.getLong(1)
//
//      var stringToLong: collection.Map[String, Long] = buffer2.getMap[String,Long](0)
//
//      val stringToLong1: collection.Map[String, Long] = buffer1.getMap[String,Long](0)
//
//      stringToLong =  stringToLong.foldLeft(stringToLong1){
//        case (map,(k,v)) => {
//
//          val newCount: Long = map.getOrElse(k,0L) + v
//          val stringToLong2: collection.Map[String, Long] = map.updated(k,newCount)
//          stringToLong2
//        }
//      }
//      buffer1.update(0,stringToLong)
//      buffer1.update(1,sun1 + sun2)
//    }
//
//    override def evaluate(buffer: Row): Any = {
//      val city: collection.Map[String,Long] = buffer.getMap[String,Long](0)
//      val count = buffer.getLong(1)
//
//      val newCount  = city.toList.sortWith((left,right)=>left._2 > right._2).take(2)
////      println(newCount.toBuffer)
//      val list = ListBuffer[String]()
//      var sum = 0L
//      newCount.foreach{
//        case (city,click) => {
//          val bil: Long = click * 100 / count
//          list.append(city+" "+ bil + "%")
//          sum += bil
//        }
//      }
//      if (city.size > 2) {
//        list.append("其他"+ (100 - sum ) +"%" )
//      }
//      list.mkString(",")
//
//    }
//  }
}
