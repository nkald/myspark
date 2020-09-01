package com.spark
import scala.collection.mutable.HashMap
import scala.math._
object Test {

  def main(args: Array[String]): Unit = {
    val ints = Array(1).toBuffer

    val ints1 = Array(1)

   val iterator: Iterator[Array[Int]] = ints1.sliding(2)

    iterator.foreach(f=>println(f.toBuffer))


//    println(ints1.sliding(2))

//    val array = Array(ints,ints1)
//    val iterator = array.map(f=>f.sliding(2))
//    iterator.foreach(f=>println(f.toBuffer))
//
//    def sum(args:Int*) = {
//      var r = 0 ;
//      for(arg <- args) r += arg ;
//     r
//    }
//
//
//
//
//
//
////    import java.lang._
//    val a = 100;
//    sqrt(100)
//
//    var b = "3";b = "6"
//
//    var d:Int = 33;d = 6
//
//    def cont(n:Int): Unit = {
//      0 to n foreach(println)
//    }
//    cont(10)
//
//    val tokens = "one two three four two two three four".split(" ")
//    val map = new HashMap[String,Int]
//    for(key <- tokens){
//      map(key) = map.getOrElse(key,0) + 1
//    }
//println(map)
//    println(Array("one", "two", "three").max)
//
//
//    def getGoodsPrice(goods:String) =
//    {
//      val prices = Map("book" -> 5, "pen" -> 2, "sticker" -> 1)
//      prices.getOrElse(goods, 0)


//    val map: Map[String, Int] = Map("book" -> 5, "pen" -> 2)
//      .map(m => m._1 -> m._2 * 2)
//
//    print(map)
  }




}
