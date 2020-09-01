package com.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql._

object SparkSql03 {
  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("app")
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    import spark.implicits._
    val df: DataFrame = spark.read.json("input/employees.json")
    df.createTempView("user")

    val ds: Dataset[User] = df.as[User]

    ds.select("salary").show()

    val daf = new MyAvgUDaf1



    ds.select(daf.toColumn).show()

    spark.stop()
  }
  case class Buff(var sum:Long,var cnt:Long)
  case class User(name:String,salary:Long)
//泛型
  //INT  :LONG
  //BUFF  :Buff
  //OUT  :LONG

  class MyAvgUDaf1 extends Aggregator[User,Buff,Long] {
  //缓冲区初始值
  override def zero: Buff =  Buff(0,0)
  //数据聚合

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

  override def reduce(b: Buff, a: User): Buff = {
    b.sum += a.salary
    b.cnt += 1
    b
  }
}
}
