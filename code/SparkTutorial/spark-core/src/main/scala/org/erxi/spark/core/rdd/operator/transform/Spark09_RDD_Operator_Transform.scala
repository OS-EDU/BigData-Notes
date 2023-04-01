package org.erxi.spark.core.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

/**
 * distinct: 去除重复的关键字
 */
object Spark09_RDD_Operator_Transform {

  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("distinct"))

    sc.makeRDD(List(1, 2, 3, 4, 1, 2, 3, 4))
      .distinct().collect().foreach(println)

    sc.stop()

  }
}
