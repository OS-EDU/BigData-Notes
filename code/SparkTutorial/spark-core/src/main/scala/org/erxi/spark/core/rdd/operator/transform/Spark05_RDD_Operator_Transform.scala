package org.erxi.spark.core.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

/**
 * glom: 将同一个分区的数据直接转换为相同类型的内存数组进行处理，分区不变
 */
object Spark05_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {

    val sc = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("glom"))

    val glomRDD = sc.makeRDD(List(1, 2, 3, 4), 2).glom()

    // List => Int
    // Int => Array
    glomRDD.collect().foreach(data => println(data.mkString(",")))

    println("=================")

    // 计算所有分区最大值求和（分区内取最大值，分区间最大值求和）
    val maxRDD = glomRDD.map(array => {
      array.max
    })
    println(maxRDD.collect().sum)

    sc.stop()
  }
}
