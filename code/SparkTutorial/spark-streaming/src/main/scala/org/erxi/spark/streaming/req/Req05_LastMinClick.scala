package org.erxi.spark.streaming.req

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}

import java.io.{File, FileWriter, PrintWriter}
import java.text.SimpleDateFormat
import scala.collection.mutable.ListBuffer

/**
 * 最近一分钟广告点击量，每十秒统计一次
 */
object Req05_LastMinClick {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("LastMinClick")
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

    // 窗口计算
    val reduceDS = adClickData.map(
      data => {
        val ts = data.ts.toLong
        val newTS = ts / 10000 * 10000
        (newTS, 1)
      }
    ).reduceByKeyAndWindow((x: Int, y: Int) => {
      x + y
    }, Seconds(60), Seconds(10))
    reduceDS

    reduceDS.foreachRDD(
      rdd => {
        val list = ListBuffer[String]()

        val datas: Array[(Long, Int)] = rdd.sortByKey(true).collect()
        datas.foreach {
          case (time, cnt) => {

            val timeString = new SimpleDateFormat("mm:ss").format(new java.util.Date(time.toLong))

            list.append(s"""{"xtime":"${timeString}", "yval":"${cnt}"}""")
          }
        }

        // 输出文件
        val out = new PrintWriter(new FileWriter(new File("input/adclick/adclick.json")))
        out.println("[" + list.mkString(",") + "]")
        out.flush()
        out.close()
      }
    )

    ssc.start()
    ssc.awaitTermination()
  }
}
