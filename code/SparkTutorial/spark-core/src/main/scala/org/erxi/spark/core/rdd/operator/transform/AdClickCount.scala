package org.erxi.spark.core.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

object AdClickCount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Ad Click Count").setMaster("local[*]")
    val sc = new SparkContext(conf)

    // 读取 agent.log 文件并将其转换为 RDD
    val logRDD = sc.textFile("input/agent.log")

    // 将每行日志按照指定格式转换为元组 (省份-广告，1)
    val provinceAdClickRDD = logRDD.map(line => {
      val fields = line.split(" ")
      val province = fields(1)
      val ad = fields(4)
      ((province, ad), 1)
    })

    // 按照省份和广告进行聚合，得出每个省份每个广告被点击的总数
    val provinceAdClickCountRDD = provinceAdClickRDD.reduceByKey(_ + _)

    // 将每个省份的数据分别取出来，并按照点击量从大到小排序，得到 Top3 广告
    val resultRDD = provinceAdClickCountRDD.groupBy(_._1._1).mapValues(iter => {
      iter.toList.sortWith((x, y) => x._2 > y._2).take(3)
    })

    // 打印结果
    resultRDD.collect().foreach(println)

    sc.stop()
  }
}

