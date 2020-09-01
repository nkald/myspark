package com.sparkStreaming

import java.io.{BufferedReader, InputStreamReader}
import java.net.Socket

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.{InputDStream, ReceiverInputDStream}
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.streaming.{Seconds, StreamingContext}


object deam03 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("sql").setMaster("local[*]")
    val ssc = new StreamingContext(conf,Seconds(3))
    val value: ReceiverInputDStream[String] = ssc.receiverStream(new MyReceiver("hadoop102",9999))
    value.print()
    ssc.start()
    ssc.awaitTermination()
  }
  class MyReceiver(host:String,port:Int) extends Receiver[String](StorageLevel.MEMORY_ONLY) {
    private var socket:Socket = _

    def receive(): Unit ={
      val reader = new BufferedReader(
        new InputStreamReader(socket.getInputStream,"utf-8")
      )
      while (true) {
        store(reader.readLine())
        Thread.sleep(1000)
      }
    }
    override def onStart(): Unit = {
      socket =  new Socket(host,port)
      new Thread("soctet recevier") {
        override def run(): Unit = {receive()}
      }.start()
    }
    override def onStop(): Unit = {
      if (socket != null) {
        socket.close()
        socket = null
      }
    }
  }

}
