package com.sparkStreaming

import java.io.{BufferedReader, InputStreamReader}
import java.net.Socket

import com.sparkStreaming.deam03.MyReceiver
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.{DStream, InputDStream, ReceiverInputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.streaming.{Seconds, StreamingContext}


object deam04 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("sql").setMaster("local[1]")
    val ssc = new StreamingContext(conf,Seconds(3))

    val kafkaPara:Map[String,Object] = Map[String,Object](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "hadoop102:9092,hadoop103:9092,hadoop104:9092",
      ConsumerConfig.GROUP_ID_CONFIG -> "atguigu",
      "key.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
      "value.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer"
    )

    ssc.checkpoint("cpp")
    val kafkadata: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String,String](ssc, LocationStrategies.PreferBrokers, ConsumerStrategies.Subscribe[String,String](Set("atguigu0421"),kafkaPara))

    val mapDs: DStream[(String, Int)] = kafkadata.map(_.value()).flatMap(_.split(" ")).map((_, 1))

    mapDs.updateStateByKey(
      (seq:Seq[Int],buffer:Option[Int]) =>{
        val newCount: Int = seq.sum + buffer.getOrElse(0)
        Option(newCount)
      }
    ).saveAsTextFiles("output01/outpu")


    ssc.start()
    ssc.awaitTermination()

  }

}
