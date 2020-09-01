package com.sparkstreamingproject.req

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}

import scala.collection.mutable.ListBuffer
import scala.util.Random


object GenMockData {
  def main(args: Array[String]): Unit = {

    val topic = "atguigu0421"
    val prop = new Properties()
    prop.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"hadoop102:9092")
    prop.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer")
    prop.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer")

    val producer = new KafkaProducer[String,String](prop)
    val value = new ProducerRecord[String,String](topic,"data")

    while (true){
      for (data <- genData()){
        producer.send( new ProducerRecord[String,String](topic,data))
        println(data)
      }
      Thread.sleep(2000)
    }
  }
  private def genData(): ListBuffer[String] ={
    val areaList = List("华北","东北","华南")
    val cityList = List("北京","上海","深圳")

    val buffer = ListBuffer[String]()
    for (i <- 0 to new Random().nextInt(50))  {
      val area = areaList(new Random().nextInt(3))
      val city = cityList(new Random().nextInt(3))
      val userId: Int = new Random().nextInt(6) + 1
      val adid: Int = new Random().nextInt(6) + 1


      buffer.append(s"${System.currentTimeMillis()} ${area} ${city} ${userId} ${adid}")
    }
    buffer
  }
}
