package com.sparkStreaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}


object deam07 {
  def main(args: Array[String]): Unit = {

    val context = StreamingContext.getOrCreate("ouput/haha", () => {
      val conf: SparkConf = new SparkConf().setAppName("sql").setMaster("local[*]")
      val ssc = new StreamingContext(conf, Seconds(3))
      ssc.checkpoint("ouput/haha")

      val value: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop102", 9999)
      val value1: DStream[(String, Int)] = value.flatMap(_.split(" ")).map((_, 1))
      ssc.checkpoint("ouput/haha")
      value1.updateStateByKey {
        //将相同key的value封装成Seq
        //Option 表示将缓冲区中的相同的key的聚合的值,可能存在，可能不存在
        (seq: Seq[Int], option: Option[Int]) => {
          val newCount: Int = seq.sum + option.getOrElse(0)
          Option(newCount)
        }
      }.print()
      ssc
    }
    )

    context.start()
    context.awaitTermination()
  }
}