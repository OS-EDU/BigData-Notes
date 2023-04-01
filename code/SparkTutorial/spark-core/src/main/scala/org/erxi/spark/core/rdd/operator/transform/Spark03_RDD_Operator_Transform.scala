package org.erxi.spark.core.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

/**
 * mapPartitionsWithIndex：获取第二分数据分区的数据
 */
object Spark03_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {

    val sc = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("mapPartitionsWithIndex"))

    val rdd = sc.makeRDD(List(1, 2, 3, 4), 3)

    rdd.mapPartitionsWithIndex(
      (index, iter) => {
        if (index == 1) {
          iter
        } else {
          Nil.iterator
        }
      }
    ).collect().foreach(println)

    println("======================")

    /**
     * 查看数据分区
     */
    rdd.mapPartitionsWithIndex(
      (index, iter) => {
        iter.map(num => {
          (index, num)
        })
      }
    ).collect().foreach(println)

    sc.stop()
  }
}
