package com.action

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.{After, Before, Test}

class Day04 {

  //配置文件
  var context: SparkContext = _

  @Before
  def init(): Unit = {
    context = new SparkContext(new SparkConf().setAppName("myapp").setMaster("local[*]"))
    val system = FileSystem.newInstance(new Configuration())
    system.delete(new Path("output"), true)
  }

  @After
  def before(): Unit = {
    context.stop
  }

  @Test
  def testRdd(): Unit = {

    val arr = Array(1, 2, 3, 4)

    val rdd = context.makeRDD(arr, 2)
    val sum1 = context.longAccumulator("sum")
    rdd.foreach(num => {sum1.add(num)}
    )
    println(sum1.value)
  }


}

//如何将List(("a",2),() ) 拆分 list(("a",1),("a",1),...)