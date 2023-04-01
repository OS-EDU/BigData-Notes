package org.erxi.spark.core.rdd.operator.transform

import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

/**
 * partitionBy: 将数据按照指定 Partitioner 重新进行分区。Spark 默认的分区器是 HashPartitioner
 */
object Spark14_RDD_Operator_Transform {

  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("key-value"))

    sc.makeRDD(List(1, 2, 3, 4), 2)
      .map((_, 1)).partitionBy(new HashPartitioner(2))
      .saveAsTextFile("output")

    sc.stop()
  }
}
