package com.spark

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.{After, Before, Test}

class TestRddMap {

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



  }
}
