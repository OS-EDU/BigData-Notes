package org.erxi.spark.core.wc

import org.apache.spark.{SparkConf, SparkContext}

object Spark02_WordCount {
  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    val sc = new SparkContext(sparkConf)

    val textFile = sc.textFile("input/word.txt")

    /**
     * flatMap 用于分割 将 RDD 中的每一个元素扁平化并进行映射操作
     * map 函数将其映射为 (word, 1) 的键值对
     * reduceByKey 将相同单词的键值对聚合起来
     */
    val res = textFile.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _)

    res.collect().foreach(println)

    sc.stop()
  }
}

