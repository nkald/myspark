package com.sparkStreaming

import java.io.File

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Milliseconds, Seconds, StreamingContext, StreamingContextState}


object deam08 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("sql").setMaster("local[*]")
    conf.set("spark.streaming.stopGracefullyOnShutdown", "true")
    val ssc = new StreamingContext(conf,Seconds(3))

    val value: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop102",9999)

    val ds: DStream[(String, Int)] = value.flatMap(_.split(" ")).map((_,1))

    ssc.checkpoint("output/kk")


    ds.countByWindow(Seconds(3),Seconds(6)).print()

    ssc.start()

    new Thread(() => {
      while (true) {
        Thread.sleep(10000)

        val state: StreamingContextState = ssc.getState()

        val file = new File("stop")
        if (file.exists()) {
          if (state == StreamingContextState.ACTIVE) {
            ssc.stop(true,true)
            System.exit(0)
          }
        }
      }
    }).start()
    ssc.awaitTermination()

  }

}
