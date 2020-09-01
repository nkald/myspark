package com.action

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.junit.{After, Before, Test}

class Day001 {

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

      val ints = Array(("a",1),("d",2))
      val ints1 = Array(("c",3),("d",4))

      val rdd1 = context.makeRDD(ints,1)
      val rdd2 = context.makeRDD(ints1,1)

      rdd1.reduceByKey(_+_).collect().foreach(println)
//        val i = rdd1.reduce(_+_)
//      println(i)
//      println(rdd1.count())
//
//      println(rdd1.first())
//      println(rdd1.takeOrdered(3).mkString(","))
//
//      println(rdd1.aggregate(10)(_ + _, _ + _))

    }

  class User extends Serializable


}
