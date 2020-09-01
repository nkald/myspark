/*
package com.sparkstreamingproject.req

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet}
import java.text.SimpleDateFormat
import java.util.Date

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}

import scala.collection.mutable.ListBuffer

object Req1_BlackList {

  def main(args: Array[String]): Unit = {


    // 将每天 对某个广告点击超过100 次的用户拉黑
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

    val kafkaDS: DStream[String] = kafkadata.map(_.value())

    val clickDStream: DStream[AdClickData] = kafkadata.map(recode => {
      val data = recode.value()
      val datas = data.split(" ")
      AdClickData(datas(0), datas(1), datas(2), datas(3), datas(4))
    })
    //1. 读取Kafka中广告点击的数据

    val aggregateDStream = clickDStream.transform(
      rdd => {
        val connection = JDBCUtil.init().getConnection

        val statement: PreparedStatement = connection.prepareStatement("select userid from black_list")
        val rs: ResultSet = statement.executeQuery()
        val blackids = ListBuffer[String]()
        while (rs.next()) {
          blackids.append(rs.getString(1))
        }
        rs.close()
        connection.close()
        //将黑名单数据过滤掉
       val filterRdd =  rdd.filter(data=>{!blackids.contains(data.userid)})

        val sdf = new SimpleDateFormat("yyyy-MM-dd")
        filterRdd.map(data=>{
          val date = new Date(data.ts.toLong)
          ((sdf.format(date),data.userid,data.adid),1)
        }).reduceByKey(_+_)
      }
    )

    aggregateDStream.foreachRDD{

      case ((day,userId,adid),sum)=> {
        val connection = JDBCUtil.init().getConnection

      }    }
    ssc.start()
    ssc.awaitTermination()
  }
  case class AdClickData(ts:String,area:String,city:String,userid:String,adid:String)

}
*/
