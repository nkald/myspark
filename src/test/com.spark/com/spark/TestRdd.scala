package com.spark


import java.io.File

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.{After, Before, Test}

class TestRdd {

  //配置文件

  var context: SparkContext = _
  @Before
  def init(): Unit = {
    context  = new SparkContext(new SparkConf().setAppName("myapp").setMaster("local[*]"))
    val system = FileSystem.newInstance(new Configuration())
    system.delete(new Path("output"),true)
  }
  @After
  def before(): Unit = {
    context.stop
  }



  @Test
  def testRdd(): Unit = {

    val ints = Array(1,2,3,4,5,6)

    val rdd1 = context.makeRDD(ints,2)
//    unit.map(f=>f+2).collect().foreach(println)

    val rdd2: RDD[String] = rdd1.map(f=>f+"第一次")


    val rdd3 = rdd2.map(f => f+"第二次")

    rdd3.map(f=>f+"第三次").collect().foreach(println)



  }
}
