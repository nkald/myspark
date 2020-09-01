package com.sparkStreaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object deam01 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("Streaming").setMaster("local[*]")

    val ssc = new StreamingContext(conf,Seconds(3))

    val dsream: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop102",9999)

    val wordStream: DStream[String] = dsream.flatMap(_.split(" "))

    val wordToOne: DStream[(String, Int)] = wordStream.map((_,1))

    val wordCount: DStream[(String, Int)] = wordToOne.reduceByKey(_+_)
    wordCount.print()
    ssc.start()
    ssc.awaitTermination()

//    ssc.stop()

  }

}
