package com.action

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
import org.junit.{After, Before, Test}

class Day05 {


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
      val list = List(1,2,3,4,5,6)
//      context.makeRDD(list,2).filter(_%2 == 0).saveAsTextFile("output")
    /*  context.makeRDD(list,2).mapPartitionsWithIndex(
        (index,iter)=> {
          if (index == 1){
            iter
          }else {
            Nil.iterator
          }

        }
      ).collect().foreach(println)

     */

      val rdd: RDD[Array[Int]] = context.makeRDD(list,2).glom()

//      println(rdd.map(arr => arr.max).sum())

//      context.makeRDD(list).sample(false,0.5).collect().foreach(println)

//      context.makeRDD(list,2).coalesce(4,true).saveAsTextFile("output")
//      context.makeRDD(list).map(f=>(f,1)).partitionBy(new HashPartitioner(5)).saveAsTextFile("output")
      val list1 = List(1,2,2,4,5,6)

      //context.makeRDD(list1).map(f=>(f,1)).reduceByKey(_+_).collect().foreach(println)
      val list2 = List(("a",1),("b",2),("c",2),("c",5))
////      context.makeRDD(list2).groupByKey().collect().foreach(println)
//      context.makeRDD(list2,3).aggregateByKey("0")(
//        (p,y) => p+y,
//        (x,y) => x + y
//      ).collect().foreach(println)
/*
      context.makeRDD(list2,2).groupByKey().mapValues(
        f=>{
          val l = f.toList
          l.sum.toDouble / l.size
        }
      ).collect().foreach(println)
 */
      context.makeRDD(list2,2).combineByKey(
        f=>(f,1),
        (p:(Int,Int),v) =>(p._1+v,p._2+1),
        (p:(Int,Int),p2:(Int,Int)) => (p._1 + p2._1,  p._2 + p2._2)
      ).map(f=>(f._1, f._2._1 / f._2._2)).collect().foreach(println)
    }

    class User extends Serializable




}
