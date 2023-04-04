package org.erxi.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkStreaming09_Resume {

  def main(args: Array[String]): Unit = {
    val ssc = StreamingContext.getActiveOrCreate("cp", () => {
      val sparkConf = new SparkConf().setMaster("local[*]").setAppName("resume")
      val ssc = new StreamingContext(sparkConf, Seconds(5))

      val lines = ssc.socketTextStream("localhost", 9999)
      val wordToOne = lines.map((_, 1))

      wordToOne.print()

      ssc
    })

    ssc.checkpoint("cp")

    ssc.start()
    ssc.awaitTermination()
  }
}
