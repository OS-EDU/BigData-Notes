package org.erxi.spark.core.wc

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object Spark01_WordCount {
  // main 方法，程序从这里开始执行
  def main(args: Array[String]): Unit = {

    // 创建一个 SparkConf 对象，设置应用名称为 "WordCount"，设置本地模式启动（使用所有可用 CPU 核心）
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")

    // 创建一个 SparkContext 对象
    val context = new SparkContext(sparkConf)

    // 定义输入文件路径
    val inputPath = "input/word.txt"
    // 使用 textFile 方法读取文件并返回 RDD
    val lines: RDD[String] = context.textFile(inputPath)

    // 利用 flatMap 方法把每一行文本拆分成单词，并将所有单词放到同一个 RDD 中
    val words = lines.flatMap(_.split(" "))

    // 使用 groupBy 方法把相同的单词放到同一个组中
    val wordGroup = words.groupBy(word => word)

    // 使用 map 方法遍历所有单词和它们出现的次数，并将结果存储到元组中
    val wordToCount = wordGroup.map {
      case (word, list) =>
        (word, list.size)
    }

    // 把 RDD 转换成数组并打印出来
    val array = wordToCount.collect()
    array.foreach(println)

    // 停止 SparkContext 的运行
    context.stop()

  }
}
