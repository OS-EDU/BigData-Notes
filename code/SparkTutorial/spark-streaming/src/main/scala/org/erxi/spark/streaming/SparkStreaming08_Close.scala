package org.erxi.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext, StreamingContextState}

object SparkStreaming08_Close {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("close")
    val ssc = new StreamingContext(sparkConf, Seconds(3))

    val lines = ssc.socketTextStream("localhost", 9999)
    val wordToOne = lines.map((_, 1))

    wordToOne.print()

    ssc.start()


    new Thread(new Runnable {
      override def run(): Unit = {
        Thread.sleep(5000)
        val state = ssc.getState()
        if (state == StreamingContextState.ACTIVE) {
          ssc.stop(true, true)
        }
        System.exit(0)
      }
    }).start()

    ssc.awaitTermination() // block 阻塞 main 线程
  }

}
