package com.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.{DataFrame, Dataset, Encoder, Encoders, Row, SparkSession, functions}

object SparkSql02 {
  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("app")
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    import spark.implicits._

    val df: DataFrame = spark.read.json("input/employees.json")
    df.createTempView("user")



    //将强类型聚合函数转换为弱类型聚合函数
    spark.udf.register("ageAvg",functions.udaf(new MyAvgUDaf))
    spark.sql("select ageAvg(salary) from user").show()
    spark.stop()
  }
  case class Buff(var sum:Long,var cnt:Long)
  case class User(name:String,salary:Long)
//泛型
  //INT  :LONG
  //BUFF  :Buff
  //OUT  :LONG
  class MyAvgUDaf extends Aggregator[Long,Buff,Long] {
  //缓冲区初始值
  override def zero: Buff =  Buff(0,0)
  //数据聚合
  override def reduce(buff: Buff, input: Long): Buff = {
    buff.sum += input
    buff.cnt += 1
    buff
  }
  //缓冲区之间的聚合
  override def merge(b1: Buff, b2: Buff): Buff = {
    b1.sum += b2.sum
    b1.cnt += b2.cnt
    b1
  }
  //完成计算
  override def finish(reduction: Buff): Long = {
    reduction.sum / reduction.cnt
  }
  //SparkSql使用编码器对对向进行序列化
  override def bufferEncoder: Encoder[Buff] = Encoders.product

  override def outputEncoder: Encoder[Long] = Encoders.scalaLong
}
}
