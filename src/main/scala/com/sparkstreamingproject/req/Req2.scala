package com.sparkstreamingproject.req

import java.sql.{Connection, PreparedStatement, ResultSet}
import java.text.SimpleDateFormat
import java.util.Date

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable.ListBuffer

object Req1_BlackList {

  def main(args: Array[String]): Unit = {


    // 实时统计每天各地区各城市各广告的点击总流量，并将其存入MySQL。
    val conf: SparkConf = new SparkConf().setAppName("sql").setMaster("local[*]")
    val ssc = new StreamingContext(conf, Seconds(3))

    val kafkaPara: Map[String, Object] = Map[String, Object](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "hadoop102:9092,hadoop103:9092,hadoop104:9092",
      ConsumerConfig.GROUP_ID_CONFIG -> "atguigu0421",
      "key.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
      "value.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer"
    )

    val kafkadata: InputDStream[ConsumerRecord[String, String]]
    = KafkaUtils.
      createDirectStream[String, String](ssc, LocationStrategies.PreferBrokers, ConsumerStrategies.Subscribe[String, String](Set("atguigu0421"), kafkaPara))

    val kafkaDS: DStream[String] = kafkadata.map(_.value())

    val clickDStream: DStream[AdClickData] = kafkadata.map(recode => {
      val data = recode.value()
      val datas = data.split(" ")
      AdClickData(datas(0), datas(1), datas(2), datas(3), datas(4))
    })

    val sdf = new SimpleDateFormat("yyyy-MM-dd")
    val mapDStream: DStream[((String, String, String, String), Int)] = clickDStream.map(
      data => {
        val day = sdf.format(new Date(data.ts.toLong))
        ((day, data.area, data.city, data.adid), 1)
      }
    )
    val redeceDs: DStream[((String, String, String, String), Int)] = mapDStream.reduceByKey(_ + _)

    redeceDs.foreachRDD {
      rdd => {
        rdd.foreachPartition {
          datas => {

            val conn: Connection = JDBCUtil.init().getConnection()
            val pstat: PreparedStatement = conn.prepareStatement(
              """
                |insert into area_city_ad_count(dt,area,city,adid,count)
                |values(?,?,?,?,?)
                |on duplicate key
                |update count = count + ?
                |""".stripMargin)

            datas.foreach {
              case ((dt, area, city, adid), sum) => {
                pstat.setString(1, dt)
                pstat.setString(2, area)
                pstat.setString(3, city)
                pstat.setString(4, adid)
                pstat.setLong(5, sum)
                pstat.setLong(6, sum)
                pstat.executeUpdate()
              }

            }
            pstat.close()
            conn.close()
          }
        }

      }

    }

    ssc.start()
    ssc.awaitTermination()
  }

  case class AdClickData(ts: String, area: String, city: String, userid: String, adid: String)

}
