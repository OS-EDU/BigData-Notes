package org.erxi.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkStreaming06_State_Window {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("window")
    val ssc = new StreamingContext(sparkConf, Seconds(3))
    ssc.checkpoint("cp")

    val lines = ssc.socketTextStream("localhost", 9999)

    // 窗口的范围应该是采集周期的整数倍
    // 窗口可以滑动的，但是默认情况下，一个采集周期进行滑动
    // 这样的话，可能会出现重复数据的计算，为了避免这种情况，可以改变滑动的滑动（步长）
    //    lines.map((_, 1)).window(Seconds(6), Seconds(6)).reduceByKey(_ + _).print()

    // reduceByKeyAndWindow : 当窗口范围比较大，但是滑动幅度比较小，那么可以采用增加数据和删除数据的方式
    lines.map((_, 1)).reduceByKeyAndWindow(
      (x: Int, y: Int) => {
        x + y
      },
      (x: Int, y: Int) => {
        x - y
      },
      Seconds(9), Seconds(3)
    ).print()

    ssc.start()
    ssc.awaitTermination()
  }
}
