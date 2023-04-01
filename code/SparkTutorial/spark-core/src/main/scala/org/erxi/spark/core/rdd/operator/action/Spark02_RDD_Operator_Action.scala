package org.erxi.spark.core.rdd.operator.action

import org.apache.spark.{SparkConf, SparkContext}

object Spark02_RDD_Operator_Action {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    val rdd = sc.makeRDD(List(1, 2, 3, 4))

    // TODO - 行动算子

    // reduce
    val i: Int = rdd.reduce(_ + _)
    println(i)

    // count : 数据源中数据的个数
    val cnt = rdd.count()
    println(cnt)

    // first : 获取数据源中数据的第一个
    val first = rdd.first()
    println(first)

    // take : 获取N个数据
    val ints: Array[Int] = rdd.take(3)
    println(ints.mkString(","))

    // takeOrdered : 数据排序后，取N个数据
    val rdd1 = sc.makeRDD(List(4, 2, 3, 1))
    val ints1: Array[Int] = rdd1.takeOrdered(3)
    println(ints1.mkString(","))

    sc.stop()

  }

}
