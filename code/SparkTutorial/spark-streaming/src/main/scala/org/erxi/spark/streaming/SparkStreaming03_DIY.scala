package org.erxi.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.util.Random

object SparkStreaming03_DIY {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("DIY")
    val ssc = new StreamingContext(sparkConf, Seconds(3))

    ssc.receiverStream(new MyReceiver).print()

    ssc.start()
    ssc.awaitTermination()

  }

  /**
   * 自定义数据采集器
   * 1. 继承 Receiver 定义泛型 传递参数
   * 2. 重写方法
   */
  class MyReceiver extends Receiver[String](StorageLevel.MEMORY_ONLY) {
    private var flag = true

    override def onStart(): Unit = {
      new Thread(new Runnable {
        override def run(): Unit = {
          while (flag) {
            val message = "采集的数据为：" + new Random().nextInt(10).toString
            store(message)
            Thread.sleep(500)
          }
        }
      }).start()
    }

    override def onStop(): Unit = {
      flag = false
    }
  }
}
