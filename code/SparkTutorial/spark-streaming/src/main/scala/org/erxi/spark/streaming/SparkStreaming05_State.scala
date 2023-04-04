package org.erxi.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkStreaming05_State {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("state")
    val ssc = new StreamingContext(sparkConf, Seconds(3))
    ssc.checkpoint("cp")

    // 无状态数据操作，只对当前的采集周期内的数据进行处理
    // 在某些场合下，需要保留数据统计结果（状态），实现数据的汇总
    // 使用有状态操作时，需要设定检查点路径
    val datas = ssc.socketTextStream("localhost", 9999)

    datas.map((_, 1)).updateStateByKey(
      (seq: Seq[Int], buff: Option[Int]) => {
        val newCnt = buff.getOrElse(0) + seq.sum
        Option(newCnt)
      }
    ).print()

    ssc.start()
    ssc.awaitTermination()

  }
}
