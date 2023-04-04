package org.erxi.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.DStream

object SparkStreaming07_Output {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("output")
    val ssc = new StreamingContext(sparkConf, Seconds(3))
    ssc.checkpoint("cp")

    val lines = ssc.socketTextStream("localhost", 9999)
    val wordToOne = lines.map((_, 1))

    val windowDS: DStream[(String, Int)] =
      wordToOne.reduceByKeyAndWindow(
        (x: Int, y: Int) => {
          x + y
        },
        (x: Int, y: Int) => {
          x - y
        },
        Seconds(9), Seconds(3))

    windowDS.print()

    ssc.start()
    ssc.awaitTermination()
  }
}
