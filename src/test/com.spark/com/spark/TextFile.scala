package com.spark

import java.time.format.DateTimeFormatter
import java.time.temporal.TemporalAccessor

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, Partitioner, SparkConf, SparkContext}
import org.junit.{After, Before, Test}

class TextFile {

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

    val rdd1 = context.textFile("input/apache.log",2)

    rdd1.map(f=>{
      val strings = f.split(" ")
      strings(6)
    }).collect().foreach(println)
//    rdd1.map(f=>f.split(" ")).map(f=>f(6)).collect().foreach(println)

//    rdd1.saveAsTextFile("output")
  }


  @Test
  def testRdd1(): Unit = {

  val ints = Array(1,2,3,4)
  context.makeRDD(ints,2).mapPartitions(f=>{
    Array(f.max).toIterator
  }).collect().foreach(println)

  }
  @Test
  def testRdd3: Unit = {
      val ints = Array(1,3,4,5)

    context.makeRDD(ints,2).mapPartitionsWithIndex((index,iter)=> {
      val pot: Iterator[(Int, Int)] = iter.map(mun => (index,mun))
      pot.filter(p=>p._1 == 0)
    }).collect().foreach(println)
  }


  @Test
  def testFlagMap: Unit = {
    val list = List(List(1,2),3,List(4,5))

//      val arr = Array(1,2,3,4)
      val arr = Array("hello")
    context.makeRDD(list).flatMap( f => {
      f match {
        case f:List[Int] => f
        case _ => List(f)
      }
    }

    ).collect().foreach(println)

  }

  @Test
  def testGlom: Unit = {
      val arr: Array[Int] = Array(1,2,3,4,18,89,12)
    val rdd: RDD[Array[Int]] = context.makeRDD(arr,2).glom()

    println(rdd.map(f => {
      f.max
    }).collect().sum)


//    rdd.collect().foreach(println)

  }

  @Test
  def testGroup: Unit = {
      val ints = Array(1,2,3,4,5)
    context.makeRDD(ints).groupBy(f=>"1",3).collect().foreach(println)

  }
  @Test
  def testGroupT: Unit = {
    val strings = List("Hello", "hive", "hbase", "Hadoop")

    context.makeRDD(strings).groupBy(f=>f.charAt(0),2).saveAsTextFile("output")
  }

  @Test
  def testWordcount: Unit = {

//   context.textFile("input/apache.log")
//     .map(_.split(" ")(3))
//     .groupBy(f=>f.split(":")(1))
//     .map(f=>(f._1,f._2.size)).collect().foreach(println)

    context.textFile("input/apache.log").map(_.split(" "))
      .filter(line => line(3).startsWith("17/05/2015")).collect().foreach(f=>println(f.mkString(",")))
  }

  @Test
  def testSample: Unit = {
      //采样
    val ints = Array(1,2,3,4,5,6,7,8,9)
//    context.makeRDD(List((1,2),(3,4))).reduceByKey((a,b)=>a)

    context.makeRDD(ints)
      .sample(true,2).collect().foreach(println)
  }

  @Test
  def testConse: Unit = {
      val ints = Array(1,2,3,4,5,6,7,8)
    context.makeRDD(ints,10).coalesce(3,true).saveAsTextFile("output")


  }

  @Test
  def testSort: Unit = {
    val ints = Array(1,2,3,4,5,6,7,8)
    context.makeRDD(ints,2).sortBy(f=>f,false,4).saveAsTextFile("output")
  }
  @Test
  def testJao: Unit = {
      val arr1 = Array(1,2,3,4)
      val arr2 = Array("2","3","4","5")

    val rdd1 = context.makeRDD(arr1)
    val rdd2 = context.makeRDD(arr2)
    rdd1.zip(rdd2).collect().foreach(println)

//    rdd2.subtract(rdd2)
//    rdd1.union(rdd2).collect().foreach(println)
  }

  @Test
  def testPartitionW: Unit = {
    val arr1 = Array(1,2,3,4)
    val arr2 = Array("2","3","4","5")

     context.makeRDD(arr1,2).map(f=>(f,1)).partitionBy(new HashPartitioner(2))
      .saveAsTextFile("output")
  }

  @Test
  def testZdingPartition: Unit = {

    val arr = List(
      ("nba", "xxx"),
      ("cba", "xxx"),
      ("wnba", "xxx"),
      ("nba", "xxx"),
      ("cba", "xxx")
    )

    context.makeRDD(arr).partitionBy(new mypartition).saveAsTextFile("output")
  }

  @Test
  def testGroupBykey: Unit = {

    val arr = List(
      ("nba", "xxx"),
      ("cba", "xxx"),
      ("wnba", "xxx"),
      ("nba", "xxx"),
      ("cba", "xxx")
    )

    context.makeRDD(arr).groupByKey().collect().foreach(println)
  }
  @Test
  def testAgre: Unit = {
    val arr = List(
      ("a",2), ("a", 3), ("c", 4),
      ("b", 5), ("c", 6), ("c", 7)
    )

  context.makeRDD(arr,2).foldByKey(0)(_+_).collect().foreach(println)

    //      .aggregateByKey(0)((a,b)=>Math.max(a,b),(c,d)=>c+d).collect().foreach(println)



  }

  @Test
  def testPingjun: Unit = {
    val list = List(("a", 88), ("b", 95), ("a", 91), ("b", 93), ("a", 95), ("b", 98))
    context.makeRDD(list,2).groupByKey().map(f=>(f._1,(f._2.sum/f._2.size))).collect().foreach(println)

  }

  @Test
  def testConbainer: Unit = {
    val list = List(("a", 88), ("b", 95), ("a", 91), ("b", 93), ("a", 95), ("b", 98))
    context.makeRDD(list,2).combineByKey(
      f=>(f,1), //装换格式
      (p:(Int,Int),v) => (p._1+v,p._2 +1),  //分区内计算规则
      (p:(Int,Int),p2:(Int,Int)) => (p._1+p2._1,p._2+p2._2)  //分区间计算规则
    ).map(f=>(f._1,f._2._1/f._2._2)).collect().foreach(println)
  }

  @Test
  def testSortBykey: Unit = {
    val list = List(("a", 88), ("b", 95), ("a", 91), ("b", 93), ("a", 95), ("b", 98))
    context.makeRDD(list).sortByKey()
  }
  @Test
  def testJoin: Unit = {
   val list1 = List(("a",1),("b",2),("c",3))
   val list2 = List(("a",1),("b",2),("a",3))

    context.makeRDD(list1).cogroup(context.makeRDD(list2)).collect().foreach(println)

  }


  @Test
  def testMapValues: Unit = {
   val arr = Array(("a",1),("b",2),("c",3))
    context.makeRDD(arr).mapValues(f=>1).collect().foreach(println)
  }

}

class mypartition extends Partitioner{
  override def numPartitions = 3

  override def getPartition(key: Any) = {
    key match {
      case "nba" => 0
      case "cba" => 1
      case "wnba" => 2
    }
  }
}