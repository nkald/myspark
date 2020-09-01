package com.jd.app

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Test {
  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setMaster("local[1]").setAppName("My App")

    val sc = new SparkContext(conf)

    val tuples = List((2,2,3),(1,2,3),(1,3,3),(1,3,4),(2,3,3),(2,3,2))
    sc.makeRDD(tuples).sortBy(f=>f,false).collect().foreach(println)

    val rdd9= sc.makeRDD(
      List(
        "hello", "hello", "spark"
      )
    )
    val zeroValue: Map[String, Int] = Map[String,Int]()
    val wordcountMap: Map[String, Int] = rdd9.aggregate(zeroValue)(
      (map, word) => {
        val oldValue: Int = map.getOrElse(word, 0)
        map.updated(word, oldValue + 1)
      },
      (map1, map2) => {
        map1.foldLeft(map2)(
          (m2, m1) => {
            val word: String = m1._1
            val count: Int = m1._2
            val oldValue: Int = m2.getOrElse(word, 0)
            map2.updated(word, oldValue + count)
          }
        )
      }
    )
    println(wordcountMap)

//    //todo fold
//    val list = List("hello",
//      "hello",
//      "spark")
//    val rdd10: RDD[String] = sc.makeRDD(list,1)
//
//    val zeroValue1: Map[String, Int] = Map[String,Int]()
//
//    val rdd10map: RDD[Map[String, Int]] = rdd10.map(word => {
//      Map[String, Int](word -> 1)
//    })
//
//    val foldmap: Map[String, Int] = rdd10map.fold(zeroValue1)(
//      (map1, map2) => {
//        map1.foldLeft(map2)(
//          (m2, m1) => {
//            val word: String = m1._1
//            val count: Int = m1._2
//            val oldValue: Int = m2.getOrElse(word, 0)
//            map2.updated(word, oldValue + count)
//          }
//        )
//      }
//    )
//    println(foldmap)
//
//
//    val rdd11 = sc.makeRDD(
//      List(
//        "hello", "hello","spark"
//      )
//    )
//
//    val zeroValue2 = Map[String, Int]()
//
//    val wordCountMap =
//      rdd11.aggregate(zeroValue)(
//        ( map, word ) => {
//          val oldValue = map.getOrElse(word, 0)
//          map.updated( word, oldValue + 1 )
//        },
//        ( map1, map2 ) => {
//          // TODO 两个Map的合并
//          map1.foldLeft(map2)(
//            (map, kv) => {
//              val word = kv._1
//              val count = kv._2
//              val oldValue = map.getOrElse(word, 0)
//              map.updated( word, oldValue + count )
//            }
//          )
//        }
//      )
//    println(wordCountMap)

    (0 to 22).foreach { x =>
      val types = (1 to x).foldRight("RT")((i, s) => {s"A$i, $s"})
      val typeTags = (1 to x).map(i => s"A$i: TypeTag").foldLeft("RT: TypeTag")(_ + ", " + _)
      val inputEncoders = (1 to x).foldRight("Nil")((i, s) => {s"Try(ExpressionEncoder[A$i]()).toOption :: $s"})
      println(s"""
                 |/**
                 | * Registers a deterministic Scala closure of $x arguments as user-defined function (UDF).
                 | * @tparam RT return type of UDF.
                 | * @since 1.3.0
                 | */
                 |def register[$typeTags](name: String, func: Function$x[$types]): UserDefinedFunction = {
                 |  val ScalaReflection.Schema(dataType, nullable) = ScalaReflection.schemaFor[RT]
                 |  val inputEncoders: Seq[Option[ExpressionEncoder[_]]] = $inputEncoders
                 |  val udf = SparkUserDefinedFunction(func, dataType, inputEncoders).withName(name)
                 |  val finalUdf = if (nullable) udf else udf.asNonNullable()
                 |  def builder(e: Seq[Expression]) = if (e.length == $x) {
                 |    finalUdf.createScalaUDF(e)
                 |  } else {
                 |    throw new AnalysisException("Invalid number of arguments for function " + name +
                 |      ". Expected: $x; Found: " + e.length)
                 |  }
                 |  functionRegistry.createOrReplaceTempFunction(name, builder)
                 |  finalUdf
                 |}""".stripMargin)
    }

  }



}
