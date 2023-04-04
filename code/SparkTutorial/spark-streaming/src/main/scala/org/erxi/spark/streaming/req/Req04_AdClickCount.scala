package org.erxi.spark.streaming.req

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.erxi.spark.streaming.util.JDBCUtil

import java.text.SimpleDateFormat
import java.util.Date

/**
 * 实时统计每天各地区各城市各广告的点击总流量，并将其存入 MySQL。
 */
object Req04_AdClickCount {
  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("AdClickCount")
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    val kafkaPara = Map[String, Object](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "hadoop102:9092, hadoop103:9092, hadoop104:9092",
      ConsumerConfig.GROUP_ID_CONFIG -> "spark",
      "key.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
      "value.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer"
    )

    val kafkaDataDS: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](Set("mockData"), kafkaPara)
    )

    val adClickData = kafkaDataDS.map(
      kafkaData => {
        val data = kafkaData.value()
        val datas = data.split(" ")
        AdClickData(datas(0), datas(1), datas(2), datas(3), datas(4))
      }
    )

    val reduceDS = adClickData.map(
      data => {
        val simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
        val day = simpleDateFormat.format(new Date(data.ts.toLong))
        val area = data.area
        val city = data.city
        val ad = data.ad

        ((day, area, city, ad), 1)
      }
    ).reduceByKey(_ + _)

    reduceDS.foreachRDD(
      rdd => {
        rdd.foreachPartition(
          iter => {
            val conn = JDBCUtil.getConnection
            val statement = conn.prepareStatement(
              """
                | insert into area_city_ad_count ( dt, area, city, adid, count )
                | values ( ?, ?, ?, ?, ? )
                | on DUPLICATE KEY
                | UPDATE count = count + ?
                            """.stripMargin)
            iter.foreach {
              case ((day, area, city, ad), sum) => {
                println(s"${day} ${area} ${city} ${ad} ${sum}")
                statement.setString(1, day)
                statement.setString(2, area)
                statement.setString(3, city)
                statement.setString(4, ad)
                statement.setInt(5, sum)
                statement.setInt(6, sum)
                statement.executeUpdate()
              }
            }
            statement.close()
            conn.close()
          }
        )
      }
    )

    ssc.start()
    ssc.awaitTermination()
  }


}
