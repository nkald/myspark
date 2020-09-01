package com.project

import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object LoadDatas5 {
  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("app")

    val spark: SparkSession = SparkSession.builder().enableHiveSupport().config(conf).getOrCreate()


    spark.sql("use atguigu")

    val udaf = new MyUDAF()
    spark.udf.register("MyUdaf", udaf)

        spark.sql(
          """
            |select
            |        t2.product_name,
            |       t2.area,
            |       t2.clickCount,
            |       ud,
            |        ROW_NUMBER() over(partition by area order by t2.clickCount desc ) ronum
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


  class MyUDAF extends UserDefinedAggregateFunction {

    override def inputSchema: StructType = StructType(Array(StructField("city",StringType)))

    override def bufferSchema: StructType = StructType(Array( StructField("cityMap",MapType(StringType,LongType)),StructField("count",LongType)))

    override def dataType: DataType = StringType

    override def deterministic: Boolean = true

    override def initialize(buffer: MutableAggregationBuffer): Unit = {
      buffer.update(0,Map[String,Long]())
      buffer.update(1,0L)
    }

    override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
      val stringToLong: collection.Map[String, Long] = buffer.getMap[String,Long](0)
      val newTotal: Long = buffer.getLong(1) + 1
      val key: String = input.getString(0)
      val newCount: Long = stringToLong.getOrElse(key,0L) + 1
      val newMap: collection.Map[String, Long] = stringToLong.updated(key,newCount)
      buffer.update(0,newMap)
      buffer.update(1,newTotal)
    }

    override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
      var oldMap: collection.Map[String, Long] = buffer1.getMap[String,Long](0)
      val total: Long = buffer1.getLong(1)
      val map2: collection.Map[String, Long] = buffer2.getMap[String,Long](0)
      val count2: Long = buffer2.getLong(1)

      oldMap =  oldMap.foldLeft(map2){
        case (map,(k,v)) => {
          val newClick: Long = map.getOrElse(k,0L) + v
          val stringToLong: collection.Map[String, Long] = map.updated(k,newClick)
          stringToLong
        }
      }
      buffer1.update(1,count2 + total)
      buffer1.update(0,oldMap)
    }

    override def evaluate(buffer: Row): Any = {
      val city: collection.Map[String,Long] = buffer.getMap[String,Long](0)
      val count = buffer.getLong(1)

      val newCount  = city.toList.sortWith((left,right)=>left._2 > right._2).take(2)
      //      println(newCount.toBuffer)
      val list = ListBuffer[String]()
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
  }
}
