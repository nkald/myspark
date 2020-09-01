package com.spark

import org.apache.spark.{SparkConf, SparkContext}

object TestGuang {

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setMaster("local").setAppName("My App")

    val context = new SparkContext(conf)

    /*
    1516609143867 6 7 64 16
    1516609143869 9 4 75 18
    统计出每一个省份,广告被点击数量排行的Top3
     */
    context.textFile("input/agent.log").map(f=>{
            val str: Array[String] = f.split(" ")
            (str(1),str(4))
          }).groupBy(f=>f).map(f=>{
      val s: String = f._1._1
      val g: String = f._1._2
      val counnt: Int = f._2.size

      (s,(g,counnt))
    }).groupByKey().mapValues(
      f=>{
        f.toList.sortWith(
          (left,right) => left._2 > right._2
        ).take(3)
      }
    ).collect().foreach(println)


//    context.textFile("input/agent.log").map(f=>f.split(" "))
//      .map(f=>(f(1),f(4))).groupBy(f=>f).map(f=>(f._1,f._2.size)).map(f=>(f._1._1,(f._1._2,f._2))).groupByKey().mapValues(
//      f=>{
////        val list: List[(String, Int)] = f._2.toList.sortWith((x,y)=>x._2 > y._2).take(3)
////        (f._1,list)
//        f.toList.sortWith(
//          (left,right) => left._2 > right._2
//        ).take(3)
//      }
//    ).collect().foreach(println)


//    context.textFile("input/agent.log").map(f=>{
//      val str: Array[String] = f.split(" ")
//      (str(1),str(4))
//    }).groupBy(f=>f).map(f=>{
//     val s = f._1._1
//      val g = f._1._2
//      val count = f._2.toList.size
//      (s,(g,count))
//    }).groupByKey().mapValues(
//      f => {
//        f.toList.sortWith(
//          (left,right) => left._2 > right._2
//        ).take(3)
//      } ).collect().foreach(println)













//    val list = List("hello","java","scala","java","python","hello")
//    val rdd = context.makeRDD(list)
//    //groupBy
////    rdd.groupBy(f=>f).map(f=>(f._1,f._2.size)).collect().foreach(println)
//    //groupBykey
////    rdd.map(f=>(f,1)).groupByKey().map(f=>(f._1,f._2.size)).collect().foreach(println)
//    //aggregateBykey
//    rdd.map(f=>(f,1)).aggregateByKey(0)(
//      _ + _ ,
//      _+_
//    ).collect().foreach(println)
//    //foldBykey
//    rdd.map(f=>(f,1)).foldByKey(0)(_+_).collect().foreach(println)
//    //combineBykey
//
//    rdd.map(f=>(f,1)).combineByKey(
//          x=>x,
//      (k1:Int,k2:Int) => k1 + k2,
//      (k1:Int,k2:Int) => k1 + k2
//    ).collect().foreach(println)
//    //countBykey
//    println(rdd.map(f => (f, 1)).countByKey())
//    //countByValue
//    println(rdd.countByValue())
//    //aggregate         aggregate[U: ClassTag](zeroValue: U)(seqOp: (U, T) => U, combOp: (U, U) => U)
//    rdd.aggregate(Map[String,Int]())(
//      (map,word) =>{
//        map.updated( word,map.getOrElse(word,0) + 1)
//      },
//      (map1,map2) =>{
//        map1.foldLeft(map2)(
//          (map,kv)=>{
//            map.updated(kv._1,kv._2 + map.getOrElse(kv._1,0))
//          }
//        )
//      }
//
//    )
//
//    //fold
//
//
//
//
//
//
//
//
//
//
//
//
//
//
//
//
////    val list1 = List(("a",1),("b",2),("c",4),("d",4))
////    val list2 = List(("a",6),("b",7),("c",8))
////
//////    val rdd1: RDD[(String, Int)] = context.makeRDD(list1)
//////    val rdd2: RDD[(String, Int)] = context.makeRDD(list2)
//////    //问题 笛卡尔集 ，
//////    rdd1.join(rdd2).collect().foreach(println)
////    val list3 = Map(("a",7),("b",8),("c",9),("d",14))
////    val rdd1: RDD[(String, Int)] = context.makeRDD(list1)
////    val bc: Broadcast[Map[String, Int]] = context.broadcast(list3)
////    rdd1.map{
////      case (x,y) => {
////        (x,(y,bc.value.getOrElse(x,0)))
////      }
////    }.collect().foreach(println)

    val arr1 = Array(("a",1),("b",2),("c",3))
    context.makeRDD(arr1,2)


    val arr2 = Array(("a",1),("b",2),("c",3))


//    arr1.fold(arr2)((f1:(String,Int),f2:(String,Int))=> (f1._1,f1._2 + f2._2))



    val arr3 = Array("a","b","c","d")
    val arr4 = Array(1,2,3,4)

    println(arr4.scan(0)(_ + _).mkString(","))

//    val functionToStrings: ((String, String) => String) => Array[String] = arr3.scan("1")((a:String,b:String)=> a + b)

//    println(functionToStrings)







  }
}
