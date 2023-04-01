package org.erxi.spark.core.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

/**
 * 双 value 类型：intersection、union、subtract、zip
 */
object Spark13_RDD_Operator_Transform {

  def main(args: Array[String]): Unit = {

    val sc = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("value-value"))

    // 交集，并集和差集要求两个数据源数据类型保持一致
    // 拉链操作两个数据源的类型可以不一致

    val rdd1 = sc.makeRDD(List(1, 2, 3, 4))
    val rdd2 = sc.makeRDD(List(3, 4, 5, 6))
    val rdd3 = sc.makeRDD(List("3", "4", "5", "6"))

    // 交集
    println(rdd1.intersection(rdd2).collect().mkString(","))

    // 并集     println(rdd1.union(rdd2).collect().mkString(","))
    println(rdd1.union(rdd2).collect().distinct.mkString(","))

    // 差集
    println(rdd1.subtract(rdd2).collect().mkString(","))

    // 拉链 【1-3，2-4，3-5，4-6】
    // Can only zip RDDs with same number of elements in each partition
    // Can't zip RDDs with unequal numbers of partitions: List(2, 4)
    println(rdd1.zip(rdd2).collect().mkString(","))

    println(rdd1.zip(rdd3).collect().mkString(","))


    sc.stop()

  }
}
