package com.sparkStreaming

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream, ReceiverInputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}


object deam05 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("sql").setMaster("local[*]")
    val ssc = new StreamingContext(conf,Seconds(3))


    val value: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop102",9999)
    val value1: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop102",8888)


    val r1: DStream[(String, Int)] = value.map((_,1))
    val r2: DStream[(String, Int)] = value1.map((_,1))

    val value2: DStream[(String, (Int, Int))] = r1.join(r2)

    value2.print()


    ssc.start()
    ssc.awaitTermination()
  }

}
