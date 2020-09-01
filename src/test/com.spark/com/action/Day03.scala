package com.action

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.{After, Before, Test}

class Day03 {

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

     val arr = Array(1,2,3,4)

      val value = context.makeRDD(arr)

      val rdd = value.map(f => {
        println("map...")
        f
      })
      rdd.cache()
      context.setCheckpointDir("./output2")
      rdd.checkpoint()

      rdd.collect()
      println(rdd.toDebugString)


      rdd.filter(f=>f%2==0).collect()





    }






}

//如何将List(("a",2),() ) 拆分 list(("a",1),("a",1),...)