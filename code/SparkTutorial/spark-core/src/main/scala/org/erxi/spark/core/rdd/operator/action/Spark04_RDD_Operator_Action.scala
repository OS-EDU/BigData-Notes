package org.erxi.spark.core.rdd.operator.action

import org.apache.spark.{SparkConf, SparkContext}

object Spark04_RDD_Operator_Action {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("countByKey")
    val sc = new SparkContext(sparkConf)

    //    val rdd = sc.makeRDD(List(1, 1, 1, 4), 2)
    val rdd = sc.makeRDD(List(
      ("a", 1), ("a", 2), ("a", 3)
    ))

    // val intToLong: collection.Map[Int, Long] = rdd.countByValue()
    // println(intToLong)
    val stringToLong: collection.Map[String, Long] = rdd.countByKey()
    println(stringToLong)

    sc.stop()
  }
}
