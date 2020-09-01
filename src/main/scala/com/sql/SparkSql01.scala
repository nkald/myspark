package com.sql

import java.util.concurrent.atomic.LongAccumulator

import org.apache.hadoop.mapred.lib.aggregate.UserDefinedValueAggregatorDescriptor
import org.apache.spark.{SparkConf, util}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.expressions.{Aggregator, MutableAggregationBuffer, UserDefinedAggregateFunction, UserDefinedFunction}
import org.apache.spark.sql.types.{DataType, DoubleType, LongType, StructField, StructType}
import org.apache.spark.sql.{Column, DataFrame, Encoder, Row, SparkSession}

object SparkSql01 {
  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("app")


    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    val df: DataFrame = spark.read.json("input/employees.json")

    import spark.implicits._
    df.createTempView("user")

    spark.udf.register("jia",(name:String)=>{
      s"name :  ${name}"
    })

    val ja:(String) => String = (name : String) =>{
      name
    }
//    spark.sql("select jia(name) ,salary from user").show()

    val lis = Array(1,3,4,4,5,6)

    val value: RDD[Int] = spark.sparkContext.makeRDD(lis)

    spark.udf.register("ageAvg",new MyAvgUDaf)

    spark.sql("select ageAvg(salary) from user").show()



    spark.stop()
  }

  class MyAvgUDaf extends UserDefinedAggregateFunction{
    //输入数据类型
    override def inputSchema: StructType = {
      StructType(Array(StructField("salary",LongType)))
    }

    //缓冲区数据结构
    override def bufferSchema: StructType = {
      StructType(Array(
        StructField("totalsalary",LongType),
        StructField("salarycount",LongType))
      )

    }
    //数据类型
    override def dataType: DataType = LongType
    //函数稳定性
    override def deterministic: Boolean = true
    //缓冲区初始化操作
    override def initialize(buffer: MutableAggregationBuffer): Unit = {
      buffer.update(0,0L)
      buffer.update(1,0L)
    }
    //根据输入数据更新缓冲区
    override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
      val newsum: Long = buffer.getLong(0) + input.getLong(0)
      val newCount: Long = buffer.getLong(1) + 1
      buffer.update(0,newsum)
      buffer.update(1,newCount)
    }
    //多个缓冲区的合并操作
    override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
      val newsum: Long = buffer1.getLong(0) + buffer2.getLong(0)
      val newCount: Long = buffer1.getLong(1) + buffer2.getLong(1)
      buffer1.update(0,newsum)
      buffer1.update(1,newCount)
    }
    //计算结果
    override def evaluate(buffer: Row): Any = {
      buffer.getLong(0) / buffer.getLong(1)
    }
  }
}
