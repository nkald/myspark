package com.action

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.{After, Before, Test}

class TestPrtice {

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
  def testLainxi: Unit = {
    val arr = Array(("a",1),("a",3),("b",2),("c",3))
    context.makeRDD(arr).aggregateByKey(0)(
      (a:Int,b) => a + b,
      _+_
    ).collect()
    val rdd = context.makeRDD(arr).combineByKey(
      f => (f, 1),
      (f: (Int, Int), z) => (f._1 + z, f._2 + 1),
      (f1: (Int, Int), f2: (Int, Int)) => (f1._1 + f2._1, f2._2 + f2._2)
    )
    println(rdd.toDebugString + "---------------")
    println(rdd.dependencies + "xxxxxxxxxxxx")

    val list = List(1,3,1,4,5)

    val i: Int = context.makeRDD(list).reduce(_+_)

    val l: Long = context.makeRDD(list).count()

    val i1: Int = context.makeRDD(list).first()
    val ints: Array[Int] = context.makeRDD(list).take(20)
    val ints1: Array[Int] = context.makeRDD(list).takeOrdered(2)
    context.makeRDD(list).aggregate(0)(
      (a,b) => a+ b,
      (c,d)=>c+d
    )
    context.makeRDD(list).fold(0)(_+_)
    val intToLong: collection.Map[Int, Long] = context.makeRDD(list).countByValue()
    println(intToLong)
    val stringToLong: collection.Map[String, Long] = context.makeRDD(arr).countByKey()


    println(arr.foldLeft(0)((b, f) => (f._2 + b)))
    println(list.foldLeft(0)((b, a) => a + b))


  }

  class User(age:Int){

  }

}
