package org.erxi.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkStreaming01_WordCount {
  def main(args: Array[String]): Unit = {
    // StreamingContext 创建时需要传递两个参数：环境配置, 批处理的周期（采集周期）
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Streaming-WordCount")
    val ssc = new StreamingContext(sparkConf, Seconds(3))

    val lines = ssc.socketTextStream("localhost", 9999)
    lines.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _)
      .print()

    // 启动采集器
    ssc.start()

    // 等待采集器的关闭
    ssc.awaitTermination()
  }
}
