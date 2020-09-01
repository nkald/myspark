package com.jd.app

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.{SparkConf, SparkContext}


abstract class BaseApp  {

  val outputPath:String

  var sparkContext:SparkContext = null

  def runApp(op: => Unit) :Unit = {
    start()
    try {
      op
    }
    catch {
      case  e: Exception => println(e.getMessage)
    } finally {
      sparkContext.stop()
    }
  }



  def start(): Unit ={
    val conf: SparkConf = new SparkConf().setMaster("local").setAppName("My App")
    sparkContext = new SparkContext(conf)
    val fs:FileSystem = FileSystem.get(new Configuration())

    val path = new Path(outputPath)
    if (fs.exists(path)){
      fs.delete(path,true)
    }
  }


}
