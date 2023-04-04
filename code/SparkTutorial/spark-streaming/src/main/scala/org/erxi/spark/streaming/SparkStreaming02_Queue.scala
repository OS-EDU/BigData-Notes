package org.erxi.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable

/**
 * 循环创建几个 RDD，将 RDD 放入队列。通过 SparkStream 创建 Dstream，计算
WordCount
 */
object SparkStreaming02_Queue {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Queue")
    val ssc = new StreamingContext(sparkConf, Seconds(3))

    val rddQueue = new mutable.Queue[RDD[Int]]()

    // 通过使用 ssc.queueStream(rddQueue)来创建 DStream，每一个推送到这个队列中的 RDD，都会作为一个 DStream 处理。
    val inputStream = ssc.queueStream(rddQueue, oneAtATime = false)
    val mappedStream = inputStream.map((_, 1))
    val reducedStream = mappedStream.reduceByKey(_ + _)
    reducedStream.print()

    ssc.start()

    for (i <- 1 to 5) {
      rddQueue += ssc.sparkContext.makeRDD(1 to 300, 10)
      Thread.sleep(2000)
    }

    ssc.awaitTermination()
  }
}
