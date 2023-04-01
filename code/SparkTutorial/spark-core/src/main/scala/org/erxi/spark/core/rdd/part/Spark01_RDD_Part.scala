package org.erxi.spark.core.rdd.part

import org.apache.spark.{Partitioner, SparkConf, SparkContext}

object Spark01_RDD_Part {

  def main(args: Array[String]): Unit = {
    val sparConf = new SparkConf().setMaster("local").setAppName("part")
    val sc = new SparkContext(sparConf)

    val rdd = sc.makeRDD(List(
      ("yyr", "japan"),
      ("yyf", "us"),
      ("yyb", "china"),
      ("yyr", "japan")
    ), 3)


    val partRDD = rdd.partitionBy(new MyPartitioner)

    partRDD.saveAsTextFile("output")

    sc.stop()

  }

  class MyPartitioner extends Partitioner {
    // 分区数量
    override def numPartitions: Int = 3

    // 根据数据的 key 值返回数据所在的分区索引（从 0 开始）
    override def getPartition(key: Any): Int = {
      key match {
        case "yyr" => 0
        case "yyf" => 1
        case _ => 2
      }
    }
  }
}
