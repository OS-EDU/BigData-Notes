package org.erxi.spark.core.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

/**
 * filter: 将数据根据指定的规则进行筛选过滤，符合规则的数据保留，不符合规则的数据丢弃。
 * 当数据进行筛选过滤后，分区不变，但是分区内的数据可能不均衡，生产环境下，可能会出
 * 现数据倾斜。
 */
object Spark07_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {

    val sc = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("filter"))

    sc.makeRDD(List(1, 2, 3, 4))
      .filter(num => num % 2 != 0)
      .collect().foreach(println)

    sc.stop()

  }
}
