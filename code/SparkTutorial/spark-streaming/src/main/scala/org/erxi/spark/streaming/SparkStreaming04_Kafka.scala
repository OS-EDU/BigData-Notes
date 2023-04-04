package org.erxi.spark.streaming

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.{SPARK_VERSION, SparkConf}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkStreaming04_Kafka {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("KAFKA")
    val ssc = new StreamingContext(sparkConf, Seconds(3))

    val kafkaPara = Map[String, Object](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "hadoop102:9092, hadoop103:9092, hadoop104:9092",
      ConsumerConfig.GROUP_ID_CONFIG -> "flume",
      "key.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
      "value.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer"
    )

    val kafkaDataDS: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](Set("sparkStreaming"), kafkaPara)
    )
    kafkaDataDS.map(_.value()).print()

    ssc.start()
    ssc.awaitTermination()
  }
}
