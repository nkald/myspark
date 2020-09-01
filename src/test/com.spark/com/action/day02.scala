package com.action

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.{After, Before, Test}

class Day02 {

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

      val ints = Array("java","scala","java","hive","spark","hive")

      val rdd = context.makeRDD(ints)
      //1.groupBy
     /* rdd.groupBy(f=>f).map(f=>(f._1,f._2.size)).collect().foreach(println)
      //2.groupByKey
      rdd.map(f=>(f,1)).groupByKey().map(f=>(f._1,f._2.sum)).collect().foreach(println)
      //3.reduceByKey
         rdd.map(f=>(f,1)).reduceByKey(_+_).collect().foreach(println)
      //4.aggregateBykey

      */
      rdd.map(f=>(f,1)).aggregateByKey(0)((x,y)=>x+y,_+_).collect().foreach(println)


   //foldBykey
      rdd.map(f=>(f,1)).foldByKey(0)(_+_).collect().foreach(println)
      //combineBykey
      rdd.map(f=>(f,1)).combineByKey(
        f=>f,
        (x:Int,y)=>x+y,
        (y:Int,z:Int) =>y+z
      ).collect().foreach(println)


     //countBykey
      println(rdd.map(f => (f, 1)).countByKey())
      //countByvalue
      println(rdd.countByValue()+"----------------")
      //aggregate

      //fold
//      rdd.map(f => (f, 1)).fold()
      //reduce


    }






}
//如何将List(("a",2),() ) 拆分 list(("a",1),("a",1),...)