package com.sparkstreamingproject.req

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}

object ReceiveKafkaData {
  def main(args: Array[String]): Unit = {
    //消费kafka中指定topic
    val conf: SparkConf = new SparkConf().setAppName("sql").setMaster("local[*]")
    val ssc = new StreamingContext(conf,Seconds(3))

    val kafkaPara:Map[String,Object] = Map[String,Object](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "hadoop102:9092,hadoop103:9092,hadoop104:9092",
      ConsumerConfig.GROUP_ID_CONFIG -> "atguigu0421",
      "key.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
      "value.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer"
    )

    val kafkadata: InputDStream[ConsumerRecord[String, String]]
    = KafkaUtils.
      createDirectStream[String,String](ssc, LocationStrategies.PreferBrokers, ConsumerStrategies.Subscribe[String,String](Set("atguigu0421"),kafkaPara))

    kafkadata.map(_.value()).print()


    ssc.start()

    ssc.awaitTermination()


  }

}
