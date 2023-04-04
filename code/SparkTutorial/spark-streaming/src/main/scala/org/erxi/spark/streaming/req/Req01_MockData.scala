package org.erxi.spark.streaming.req

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}

import java.util.{Properties, Random}
import scala.collection.mutable.ListBuffer
import java.util.Random

/**
 * 生成模拟数据
 * 格式：timestamp area city userid adId
 * 含义：时间戳     地区  城市  用户  广告
 *
 * Application => Kafka => SparkStreaming => Analysis
 */
object Req01_MockData {
  def main(args: Array[String]): Unit = {
    val prop = new Properties()
    // 添加配置
    prop.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop102:9092")
    prop.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    prop.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")

    val producer = new KafkaProducer[String, String](prop)

    while (true) {
      mockData().foreach(
        data => {
          // 向 kafka 中生成数据
          val record = new ProducerRecord[String, String]("mockData", data)
          producer.send(record)
          println(data)
        }
      )
      Thread.sleep(2000)
    }
  }

  private def mockData() = {
    val list = ListBuffer[String]()
    val areaList = ListBuffer[String]("华北", "华东", "华南")
    val cityList = ListBuffer[String]("北京", "上海", "深圳")

    for (i <- 1 to new Random().nextInt(50)) {

      val area = areaList(new Random().nextInt(3))
      val city = cityList(new Random().nextInt(3))
      var userId = new Random().nextInt(6) + 1
      var adId = new Random().nextInt(6) + 1

      list.append(s"${System.currentTimeMillis()} ${area} ${city} ${userId} ${adId}")
    }

    list
  }
}
